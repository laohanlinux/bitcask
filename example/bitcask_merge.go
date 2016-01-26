package main

import (
	"os"
	"runtime/debug"
	"strconv"
	"time"

	"github.com/laohanlinux/bitcask"
	"github.com/laohanlinux/go-logger/logger"
)

func main() {
	storagePath := "exampleMergeDir"
	os.RemoveAll(storagePath)
	opts := &bitcask.Options{
		MaxFileSize: 1 << 14,
	}
	bc, err := bitcask.Open(storagePath, opts)
	if err != nil {
		logger.Fatal(err)
	}
	defer func() {
		if err := recover(); err != nil {
			logger.Error(err)
			debug.PrintStack()
		}
	}()
	defer bc.Close()
	mergeWorker := bitcask.NewMerge(bc, 5)
	mergeWorker.Start()

	size := (1 << 13)

	//	var gGroup sync.WaitGroup

	///////////////////////////
	for i := 0; i < size; i++ {
		if i%1024 == 0 {
			time.Sleep(time.Second * 1)
		}
		key := strconv.Itoa(i)
		bc.Put([]byte(key), []byte(key))
	}
	time.Sleep(time.Second * 1)
	for i := 0; i < size; i++ {
		tValue := i
		key := strconv.Itoa(i)
		value, err := bc.Get([]byte(key))
		if string(value) != strconv.Itoa(tValue) {
			logger.Fatal("value:", string(value), "tValue:", strconv.Itoa(tValue), err)
		}
	}
	//////////////////////////

	for i := 0; i < size; i++ {
		if i%2 == 0 {
			key := strconv.Itoa(i)
			value := strconv.Itoa(i + 1)
			bc.Put([]byte(key), []byte(value))
		}
	}
	time.Sleep(time.Second * 1)

	for i := 0; i < size; i++ {
		tValue := i
		if i%2 == 0 {
			tValue = i + 1
		}
		key := strconv.Itoa(i)
		value, err := bc.Get([]byte(key))
		if string(value) != strconv.Itoa(tValue) {
			logger.Error("value:", string(value), "tValue:", strconv.Itoa(tValue), err)
		}
	}

	////////////////////////////
	//gGroup.Wait()
	logger.Info("pass all test")
	time.Sleep(time.Second * 120)
}
