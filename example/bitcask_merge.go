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

	keyValues := make(map[int]string)

	for j := 0; j < 5; j++ {
		for i := 0; i < (1<<10)*5; i++ {
			key := strconv.Itoa(i + j*(1<<10))
			value := strconv.Itoa(int(time.Now().Unix()))
			bc.Put([]byte(key), []byte(value))
			keyValues[i] = value
			//logger.Info(i)
		}
		time.Sleep(time.Second * 1)
	}

	logger.Info("Put all Data")
	time.Sleep(time.Second * 30)
	for i := 0; i < (1<<10)*5; i++ {
		k := strconv.Itoa(i)
		v, _ := bc.Get([]byte(k))
		if string(v) != keyValues[i] {
			logger.Error(string(v), keyValues[i])
			os.Exit(-1)
		}
	}
	// logger.Info("Get all data")
	// // delete all data
	// for i := 0; i < (1<<10)*5; i++ {
	// 	k := strconv.Itoa(i)
	// 	//v, _ := bc.Get([]byte(k))
	// 	err := bc.Del([]byte(k))
	// 	if err != nil {
	// 		logger.Error(err)
	// 	}
	// }
	// logger.Info("Delete all data")
	// // Get all data
	// for i := 0; i < b.N/2; i++ {
	// 	k := strconv.Itoa(i)
	// 	v, err := bc.Get([]byte(k))
	// 	if err != ErrNotFound {
	// 		logger.Error(string(v), keyValues[i])
	// 	}
	// }
	// logger.Info("all data is not found")

	time.Sleep(time.Second * 120)
}
