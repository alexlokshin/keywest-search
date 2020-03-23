package main

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sync"
)

var lock sync.Mutex

// Credit goes to https://medium.com/@matryer/golang-advent-calendar-day-eleven-persisting-go-objects-to-disk-7caf1ee3d11d

var Marshal = func(v interface{}) (io.Reader, error) {
	b, err := json.MarshalIndent(v, "", "\t")
	if err != nil {
		return nil, err
	}
	return bytes.NewReader(b), nil
}

var Unmarshal = func(r io.Reader, v interface{}) error {
	return json.NewDecoder(r).Decode(&v)
}

func Save(path string, v interface{}) error {
	lock.Lock()
	defer lock.Unlock()
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	r, err := Marshal(v)
	if err != nil {
		return err
	}
	_, err = io.Copy(f, r)
	return err
}

func Load(path string, v interface{}) error {
	lock.Lock()
	defer lock.Unlock()
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	return Unmarshal(f, v)
}

func SaveBinary(path string, v interface{}) error {
	lock.Lock()
	defer lock.Unlock()

	file, err := os.Create(path)
	if err == nil {
		encoder := gob.NewEncoder(file)
		err = encoder.Encode(v)
		if err != nil {
			fmt.Printf("Error saving a binary file: %v\n", err)
		}
	} else {
		fmt.Printf("Error saving a binary file: %v\n", err)
	}
	file.Close()
	return err
}

func LoadBinary(path string, v interface{}) error {
	lock.Lock()
	defer lock.Unlock()

	file, err := os.Open(path)
	if err == nil {
		decoder := gob.NewDecoder(file)
		err = decoder.Decode(v)
		if err != nil {
			fmt.Printf("Error saving a binary file: %v\n", err)
		}
	} else {
		fmt.Printf("Error reading a binary file: %v\n", err)
	}
	file.Close()
	return err
}
