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
		MaxFileSize: 1 << 10,
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
	logger.Info("start ================")
	mergeWorker := bitcask.NewMerge(bc, 5)
	mergeWorker.Start()

	size := (1 << 13)
	for i := 0; i < size; i++ {
		if i%1024 == 0 {
			time.Sleep(time.Second * 1)
		}
		key := strconv.Itoa(i)
		bc.Put([]byte(key), []byte(key))
	}

	time.Sleep(time.Second * 1)
	for i := 0; i < size; i++ {
		if i%2 == 0 {
			key := strconv.Itoa(i)
			value := strconv.Itoa(i + 1)
			bc.Put([]byte(key), []byte(value))
		}
	}

	for i := 0; i < size; i++ {
		value_ := i
		if i%2 == 0 {
			value_ = i + 1
		}
		key := strconv.Itoa(i)

		value, _ := bc.Get([]byte(key))
		if string(value) != strconv.Itoa(value_) {
			logger.Error("value:", string(value), "value_:", strconv.Itoa(value_))
		}

	}
	time.Sleep(time.Second * 120)
}
