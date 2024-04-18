package main

import (
	"bytes"
	"io"
	"testing"
	"time"
)

func TestPathTransformFunc(t *testing.T) {
	key := "thorbestpicture"
	pathKey := CASPathTransformFunc(key)
	expectedOriginalKey := "515a10b60193410421b1c6041b88070a87df4907"
	expectedPathName := "515a1/0b601/93410/421b1/c6041/b8807/0a87d/f4907"
	if pathKey.PathName != expectedPathName {
		t.Errorf("have %s want %s", pathKey.PathName, expectedPathName)
	}

	if pathKey.Filename != expectedOriginalKey {
		t.Errorf("have %s want %s", pathKey.PathName, expectedOriginalKey)
	}
}

func TestStoreDeleteKey(t *testing.T) {
	opts := StoreOpts{
		PathTransformFunc: CASPathTransformFunc,
	}
	s := NewStore(opts)
	key := "vickyspecials"
	data := []byte("some jpg bytes")

	err := s.writeStream(key, bytes.NewReader(data))
	if err != nil {
		t.Error(err)
	}

	time.Sleep(1 * time.Second)
	if err := s.Delete(key); err != nil {
		t.Error(err)
	}
}

func TestStore(t *testing.T) {
	opts := StoreOpts{
		PathTransformFunc: CASPathTransformFunc,
	}

	s := NewStore(opts)
	key := "vickyspecials"
	data := []byte("some jpg bytes")

	err := s.writeStream(key, bytes.NewReader(data))
	if err != nil {
		t.Error(err)
	}

	r, err := s.Read(key)
	if err != nil {
		t.Error(err)
	}

	b, _ := io.ReadAll(r)

	if string(b) != string(data) {
		t.Errorf("want %s have %s", data, b)
	}

	s.Delete(key)
}
