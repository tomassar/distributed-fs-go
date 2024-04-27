package main

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
)

const defaultRootFolderName = "defaultroot"

func CASPathTransformFunc(key string) PathKey {
	hash := sha1.Sum([]byte(key))
	hashStr := hex.EncodeToString(hash[:])

	return getPathFromHashedKey(hashStr)
}

type useHashedKeysKey struct{}

func (s *Store) getPathTransformFunc(ctx context.Context) PathTransformFunc {
	useHashedKeys, ok := ctx.Value(useHashedKeysKey{}).(bool)
	if ok && useHashedKeys {
		return func(key string) PathKey {
			hash := sha1.Sum([]byte(key))
			hashStr := hex.EncodeToString(hash[:])
			return getPathFromHashedKey(hashStr)
		}
	}

	return s.PathTransformFunc
}

func getPathFromHashedKey(hashStr string) PathKey {
	blockSize := 5
	sliceLen := len(hashStr) / blockSize

	paths := make([]string, sliceLen)

	for i := 0; i < sliceLen; i++ {
		from, to := i*blockSize, (i*blockSize)+blockSize
		paths[i] = hashStr[from:to]
	}

	return PathKey{
		PathName: strings.Join(paths, "/"),
		Filename: hashStr,
	}
}

type PathTransformFunc func(string) PathKey

type PathKey struct {
	PathName string
	Filename string
}

func (p PathKey) FirstPathName() string {
	paths := strings.Split(p.PathName, "/")
	if len(paths) == 0 {
		return ""
	}

	return paths[0]
}

func (p PathKey) FullPath() string {
	return fmt.Sprintf("%s/%s", p.PathName, p.Filename)
}

type StoreOpts struct {
	//Root is the foldername of the root, containing all the folders/files of the system.
	Root              string
	PathTransformFunc PathTransformFunc
}

var DefaultPathTransformFunc = func(key string) PathKey {
	return PathKey{
		PathName: key,
		Filename: key,
	}
}

type Store struct {
	StoreOpts
}

func NewStore(opts StoreOpts) *Store {
	if opts.PathTransformFunc == nil {
		opts.PathTransformFunc = DefaultPathTransformFunc
	}

	if len(opts.Root) == 0 {
		opts.Root = defaultRootFolderName
	}

	return &Store{
		StoreOpts: opts,
	}
}

func (s *Store) readFilesGivenID(ctx context.Context, id string) ([]PathKey, error) {
	var files []PathKey
	root := filepath.Join(s.Root, id)

	err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			// Assuming the path is relative to the root directory
			relativePath := strings.TrimPrefix(path, root)
			relativePath = strings.TrimPrefix(relativePath, "/")

			// Split the path to get the filename
			paths := strings.Split(relativePath, "/")
			filename := paths[len(paths)-1]

			files = append(files, PathKey{
				PathName: relativePath,
				Filename: filename,
			})
		}
		return nil
	})

	if err != nil {
		return nil, err
	}

	return files, nil
}

func (s *Store) Has(ctx context.Context, id, key string) bool {
	pathKey := s.PathTransformFunc(key)

	fullPathWithRoot := fmt.Sprintf("%s/%s/%s", s.Root, id, pathKey.FullPath())
	_, err := os.Stat(fullPathWithRoot)

	return !errors.Is(err, os.ErrNotExist)
}

func (s *Store) Clear() error {
	return os.RemoveAll(s.Root)
}

func (s *Store) Delete(id, key string) error {
	pathKey := s.PathTransformFunc(key)

	defer func() {
		log.Printf("deleted [%s] from disk", pathKey.Filename)
	}()

	firstPathNameWithRoot := fmt.Sprintf("%s/%s/%s", s.Root, id, pathKey.FirstPathName())
	return os.RemoveAll(firstPathNameWithRoot)
}

func (s *Store) Write(ctx context.Context, id, key string, r io.Reader) (int64, error) {
	return s.writeStream(id, key, r)
}

func (s *Store) WriteDecrypt(ctx context.Context, encKey []byte, id, key string, r io.Reader) (int64, error) {
	f, err := s.openFileForWriting(id, key)
	if err != nil {
		return 0, err
	}

	n, err := copyDecrypt(encKey, r, f)
	return int64(n), err
}

func (s *Store) openFileForWriting(ctx context.Context, id, key string) (*os.File, error) {
	pathKey := s.PathTransformFunc(key)
	pathNameWithRoot := fmt.Sprintf("%s/%s/%s", s.Root, id, pathKey.PathName)
	if err := os.MkdirAll(pathNameWithRoot, os.ModePerm); err != nil {
		return nil, err
	}

	fullPathWithRoot := fmt.Sprintf("%s/%s/%s", s.Root, id, pathKey.FullPath())

	return os.Create(fullPathWithRoot)
}

func (s *Store) writeStream(ctx context.Context, id, key string, r io.Reader) (int64, error) {
	f, err := s.openFileForWriting(id, key)
	if err != nil {
		return 0, err
	}
	return io.Copy(f, r)
}

func (s *Store) Read(ctx context.Context, id, key string) (int64, io.Reader, error) {
	return s.readStream(id, key)
}

func (s *Store) readStream(ctx context.Context, id, key string) (int64, io.ReadCloser, error) {
	pathKey := s.PathTransformFunc(key)
	fullPathWithRoot := fmt.Sprintf("%s/%s/%s", s.Root, id, pathKey.FullPath())

	file, err := os.Open(fullPathWithRoot)
	if err != nil {
		return 0, nil, err
	}

	fi, err := os.Stat(fullPathWithRoot)
	if err != nil {
		return 0, nil, err
	}

	return fi.Size(), file, nil
}
