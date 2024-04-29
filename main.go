package main

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"time"

	"github.com/tomassar/distributed-fs-go/p2p"
)

func makeServer(listenAddr string, nodes ...string) *FileServer {
	tcpTransportOpts := p2p.TCPTransportOpts{
		ListenAddr:    listenAddr,
		HandshakeFunc: p2p.NOPHandshakeFunc,
		Decoder:       p2p.DefaultDecoder{},
	}
	tcpTransport := p2p.NewTCPTransport(tcpTransportOpts)

	fileServerOpts := FileServerOpts{
		EncKey:            newEncryptionKey(),
		StorageRoot:       fmt.Sprintf("%s_network", listenAddr),
		PathTransformFunc: CASPathTransformFunc,
		Transport:         tcpTransport,
		BootstrapNodes:    nodes,
	}

	s := NewFileServer(fileServerOpts)

	tcpTransport.OnPeer = s.OnPeer

	return s
}

func main() {
	s1 := makeServer(":3000", "")
	s1.ID = "server1"
	s2 := makeServer(":4000", ":3000")
	s2.ID = "server2"
	s3 := makeServer(":5000", ":3000", ":4000")
	s3.ID = "server3"

	go s1.Start()
	time.Sleep(2 * time.Second)

	go s2.Start()

	time.Sleep(2 * time.Second)

	go s3.Start()
	time.Sleep(2 * time.Second)

	ctx := context.Background()

	for i := 0; i < 3; i++ {
		key := fmt.Sprintf("music_%d", i)
		data := bytes.NewReader([]byte("my big data file here!"))
		s3.Store(ctx, key, data)

		if i%2 != 0 {
			continue
		}
		time.Sleep(10 * time.Millisecond)
		if err := s3.store.Delete(ctx, s3.ID, key); err != nil {
			log.Fatal(err)
		}
		time.Sleep(100 * time.Millisecond)
		/* 		r, err := s3.Get(ctx, key)
		   		if err != nil {
		   			log.Fatal(err)
		   		}

		   		b, err := io.ReadAll(r)
		   		if err != nil {
		   			log.Fatal(err)
		   		}

		   		fmt.Println(string(b)) */
	}

	time.Sleep(4 * time.Second)
	err := s3.Sync(ctx)
	if err != nil {
		log.Fatal(err)
	}

	/* 	time.Sleep(10 * time.Second)

	   	for i := 0; i < 2; i++ {
	   		key := fmt.Sprintf("picture_%d", i)
	   		if err := s3.Delete(key); err != nil {
	   			log.Fatal(err)
	   		}
	   		time.Sleep(100 * time.Millisecond)

	   	} */

	select {}
}
