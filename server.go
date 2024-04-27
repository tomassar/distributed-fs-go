package main

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"fmt"
	"io"
	"log"
	"sync"
	"time"

	"github.com/tomassar/distributed-fs-go/p2p"
)

type FileServerOpts struct {
	// ID of the owner of the storage, which will be used to store all files at that location
	// so we can sync all the files if needed
	ID                string
	EncKey            []byte
	ListenAddr        string
	StorageRoot       string
	PathTransformFunc PathTransformFunc
	Transport         p2p.Transport
	BootstrapNodes    []string
}

type FileServer struct {
	FileServerOpts

	peerLock sync.Mutex
	peers    map[string]p2p.Peer

	store  *Store
	quitch chan struct{}
}

func NewFileServer(opts FileServerOpts) *FileServer {
	storeOpts := StoreOpts{
		Root:              opts.StorageRoot,
		PathTransformFunc: opts.PathTransformFunc,
	}

	if len(opts.ID) == 0 {
		opts.ID = generateID()
		fmt.Printf("generated ID: %s\n", opts.ID)
	}

	return &FileServer{
		FileServerOpts: opts,
		store:          NewStore(storeOpts),
		quitch:         make(chan struct{}),
		peers:          make(map[string]p2p.Peer),
	}
}

func (s *FileServer) broadcast(msg *Message) error {
	buf := new(bytes.Buffer)
	if err := gob.NewEncoder(buf).Encode(msg); err != nil {
		return err
	}

	for _, peer := range s.peers {
		peer.Send([]byte{p2p.IncomingMessage})
		if err := peer.Send(buf.Bytes()); err != nil {
			return err
		}
	}

	return nil
}

type Message struct {
	Payload any
}

type MessageStoreFile struct {
	ID   string
	Key  string
	Size int64
}

type MessageGetFile struct {
	ID  string
	Key string
}

type MessageDeleteFile struct {
	ID  string
	Key string
}

type MessageGetMyFiles struct {
	ID string
}

func (s *FileServer) Sync() error {
	msg := Message{
		Payload: MessageGetMyFiles{
			ID: s.ID,
		},
	}

	err := s.broadcast(&msg)
	if err != nil {
		return err
	}

	for _, peer := range s.peers {
		/* 	var incomingStream byte
		if err := binary.Read(peer, binary.LittleEndian, &incomingStream); err != nil {
			return fmt.Errorf("failed to read incoming stream byte: %v", err)
		}

		if incomingStream != p2p.IncomingStream {
			return fmt.Errorf("expected incoming stream byte, got %v", incomingStream)
		}
		*/
		// Loop to receive multiple files
		for {
			// Read the file key length
			var keyLength uint32
			if err := binary.Read(peer, binary.LittleEndian, &keyLength); err != nil {
				if err == io.EOF {
					break // No more files to receive
				}
				return fmt.Errorf("failed to read file key length: %v", err)
			}

			// Read the file key
			keyBytes := make([]byte, keyLength)
			if _, err := io.ReadFull(peer, keyBytes); err != nil {
				return fmt.Errorf("failed to read file key: %v", err)
			}
			fileKey := string(keyBytes)

			if ok := s.store.Has(s.ID, fileKey); !ok {
				// Read the file size
				var fileSize int64
				if err := binary.Read(peer, binary.LittleEndian, &fileSize); err != nil {
					return fmt.Errorf("failed to read file size: %v", err)
				}

				// Read the file content
				n, err := s.store.WriteDecrypt(s.EncKey, s.ID, fileKey, io.LimitReader(peer, fileSize))
				if err != nil {
					return err
				}
				fmt.Printf("[%s] received (%d) bytes over the network from (%s)\n", s.Transport.Addr(), n, peer.RemoteAddr())
			}
		}

		peer.CloseStream()
	}
	return nil
}

func (s *FileServer) Delete(key string) error {
	err := s.store.Delete(s.ID, key)
	if err != nil {
		return err
	}

	msg := Message{
		MessageDeleteFile{
			ID:  s.ID,
			Key: hashKey(key),
		},
	}

	return s.broadcast(&msg)
}

func (s *FileServer) Get(key string) (io.Reader, error) {
	if s.store.Has(s.ID, key) {
		fmt.Printf("[%s] serving file (%s) from local disk\n", s.Transport.Addr(), key)
		_, r, err := s.store.Read(s.ID, key)
		return r, err
	}

	fmt.Printf("[%s] don't have file (%s) locally, fetching from network\n", s.Transport.Addr(), key)
	msg := Message{
		Payload: MessageGetFile{
			ID:  s.ID,
			Key: hashKey(key),
		},
	}

	if err := s.broadcast(&msg); err != nil {
		return nil, err
	}

	time.Sleep(500 * time.Millisecond)

	for _, peer := range s.peers {
		// First read the file size so we can limit the amount of bytes that we read
		// from the connection, so it will not hang
		var fileSize int64
		binary.Read(peer, binary.LittleEndian, &fileSize)
		n, err := s.store.WriteDecrypt(s.EncKey, s.ID, key, io.LimitReader(peer, fileSize))
		if err != nil {
			return nil, err
		}
		fmt.Printf("[%s] received (%d) bytes over the network from (%s)\n", s.Transport.Addr(), n, peer.RemoteAddr())

		peer.CloseStream()
	}

	_, r, err := s.store.Read(s.ID, key)
	return r, err
}

func (s *FileServer) Store(key string, r io.Reader) error {
	var (
		fileBuffer = new(bytes.Buffer)
		tee        = io.TeeReader(r, fileBuffer)
	)

	size, err := s.store.Write(s.ID, key, tee)
	if err != nil {
		return err
	}

	fmt.Println("ID: ", s.ID)
	msg := Message{
		Payload: MessageStoreFile{
			ID:   s.ID,
			Key:  hashKey(key),
			Size: size + 16,
		},
	}

	err = s.broadcast(&msg)
	if err != nil {
		return err
	}

	time.Sleep(time.Millisecond * 5)
	peers := []io.Writer{}

	for _, peer := range s.peers {
		peers = append(peers, peer)
	}

	mw := io.MultiWriter(peers...)
	mw.Write([]byte{p2p.IncomingStream})

	n, err := copyEncrypt(s.EncKey, fileBuffer, mw)
	if err != nil {
		return err
	}

	fmt.Printf("[%s] received and written (%d) to disk: \n", s.Transport.Addr(), n)

	return nil
}

func (s *FileServer) Stop() {
	close(s.quitch)
}

func (s *FileServer) OnPeer(p p2p.Peer) error {
	s.peerLock.Lock()
	defer s.peerLock.Unlock()
	s.peers[p.RemoteAddr().String()] = p

	log.Printf("[%s] connected with remote %s", s.Transport.Addr(), p.RemoteAddr())

	return nil
}

func (s *FileServer) loop() {
	defer func() {
		log.Println("file server stopped due to error or user quit action")
		s.Transport.Close()
	}()

	for {
		select {
		case rpc := <-s.Transport.Consume():
			var msg Message
			if err := gob.NewDecoder(bytes.NewReader(rpc.Payload)).Decode(&msg); err != nil {
				log.Println("decoding error: ", err)
			}

			if err := s.handleMessage(rpc.From, &msg); err != nil {
				log.Println("handle message error: ", err)
			}

		case <-s.quitch:
			return
		}
	}
}

func (s *FileServer) handleMessage(from string, msg *Message) error {
	switch v := msg.Payload.(type) {
	case MessageStoreFile:
		return s.handleMessageStoreFile(from, v)
	case MessageGetFile:
		return s.handleMessageGetFile(from, v)
	case MessageDeleteFile:
		return s.handleMessageDeleteFile(from, v)
	case MessageGetMyFiles:
		return s.handleMessageGetMyFiles(from, v)
	}

	return nil
}

func (s *FileServer) handleMessageGetMyFiles(from string, msg MessageGetMyFiles) error {
	files, err := s.store.readFilesGivenID(msg.ID)
	if err != nil {
		return err
	}

	// Create a buffer to hold all the files
	var buf bytes.Buffer

	// Iterate over each file and write it to the buffer
	for _, file := range files {
		fileSize, r, err := s.store.ReadWithHashedKey(msg.ID, file.Filename)
		if err != nil {
			log.Printf("Error reading file %s: %v", file.Filename, err)
			continue
		}

		// Write the file key to the buffer
		if err := binary.Write(&buf, binary.LittleEndian, uint32(len(file.Filename))); err != nil {
			log.Printf("Error writing file key length to buffer: %v", err)
			continue
		}
		if _, err := buf.WriteString(file.Filename); err != nil {
			log.Printf("Error writing file key to buffer: %v", err)
			continue
		}

		// Write the file size to the buffer
		if err := binary.Write(&buf, binary.LittleEndian, fileSize); err != nil {
			log.Printf("Error writing file size to buffer: %v", err)
			continue
		}

		// Write the file content to the buffer
		if _, err := io.Copy(&buf, r); err != nil {
			log.Printf("Error writing file content to buffer: %v", err)
			continue
		}
	}

	// Send the buffer to the requesting peer
	peer, ok := s.peers[from]
	if !ok {
		log.Printf("Peer %s not found", from)
		return fmt.Errorf("peer %s not found", from)
	}

	// First send the "incomingStream" byte to the peer
	peer.Send([]byte{p2p.IncomingStream})

	// Then send the aggregated files
	err = peer.Send(buf.Bytes())
	if err != nil {
		log.Printf("Error sending files to peer %s: %v", from, err)
		return err
	}

	log.Printf("Sent files to peer %s", from)

	return nil
}

func (s *FileServer) handleMessageGetFile(from string, msg MessageGetFile) error {
	if !s.store.Has(msg.ID, msg.Key) {
		return fmt.Errorf("[%s] need to serve file (%s) but not found on disk", s.Transport.Addr(), msg.Key)
	}

	fmt.Printf("[%s] serving file (%s) over the network\n", s.Transport.Addr(), msg.Key)

	fileSize, r, err := s.store.Read(msg.ID, msg.Key)
	if err != nil {
		return err
	}

	if rc, ok := r.(io.ReadCloser); ok {
		fmt.Println("closing readCloser")
		defer rc.Close()
	}

	peer, ok := s.peers[from]
	if !ok {
		return fmt.Errorf("peer %s not in map", from)
	}

	// First send the "incomingStream" byte to the peer and then we can send
	// the file size as an int64
	peer.Send([]byte{p2p.IncomingStream})
	binary.Write(peer, binary.LittleEndian, fileSize)

	n, err := io.Copy(peer, r)
	if err != nil {
		return err
	}

	fmt.Printf("[%s] written %d bytes over the network to %s\n", s.Transport.Addr(), n, from)

	return nil
}

func (s *FileServer) handleMessageStoreFile(from string, msg MessageStoreFile) error {
	peer, ok := s.peers[from]
	if !ok {
		return fmt.Errorf("peer (%s) could not be found in the peer list", from)
	}

	n, err := s.store.Write(msg.ID, msg.Key, io.LimitReader(peer, msg.Size))
	if err != nil {
		return err
	}

	fmt.Printf("written %d bytes to disk\n", n)
	peer.CloseStream()

	return nil
}

func (s *FileServer) handleMessageDeleteFile(from string, msg MessageDeleteFile) error {
	fmt.Printf("[%s] received message from %s, deleting file: %s\n", s.Transport.Addr(), from, msg.Key)
	return s.store.Delete(msg.ID, msg.Key)
}

func (s *FileServer) bootstrapNetwork() error {
	for _, addr := range s.BootstrapNodes {
		if len(addr) == 0 {
			continue
		}

		go func(addr string) {
			fmt.Printf("[%s] attempting to connect with remote: %s \n ", s.Transport.Addr(), addr)
			if err := s.Transport.Dial(addr); err != nil {
				log.Println("dial error: ", err)
			}
		}(addr)
	}

	return nil
}

func (s *FileServer) Start() error {
	fmt.Printf("[%s] starting fileserver... \n", s.Transport.Addr())

	if err := s.Transport.ListenAndAccept(); err != nil {
		return err
	}

	s.bootstrapNetwork()
	s.loop()

	return nil
}

func init() {
	gob.Register(MessageStoreFile{})
	gob.Register(MessageGetFile{})
	gob.Register(MessageDeleteFile{})
	gob.Register(MessageGetMyFiles{})
}
