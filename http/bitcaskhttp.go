package main

import (
	"flag"
	"io/ioutil"
	"net/http"
	"runtime/debug"

	"github.com/laohanlinux/bitcask"
	"github.com/laohanlinux/go-logger/logger"
	"github.com/laohanlinux/mux"
)

var addr string
var storagePath string
var merged bool
var inteval int64
var maxSize uint64
var logLevel int

var bc *bitcask.BitCask

func main() {
	flag.StringVar(&addr, "addr", "127.0.0.1:80", "bitcask http listen addr")
	flag.StringVar(&storagePath, "s", "bitcaskStorage", "data storage path")
	flag.BoolVar(&merged, "m", false, "true: open file merge; false: not open file merge ")
	flag.Int64Var(&inteval, "t", 3600, "inteval for file merging")
	flag.Uint64Var(&maxSize, "ms", 1<<32, "single data file maxsize")
	flag.IntVar(&logLevel, "l", 0, "logger level")
	flag.Parse()

	logger.SetLevel(1)
	opts := &bitcask.Options{
		MaxFileSize: maxSize,
	}
	var err error
	bc, err = bitcask.Open(storagePath, opts)
	if err != nil {
		logger.Fatal(err)
	}
	defer bc.Close()

	defer func() {
		if err := recover(); err != nil {
			logger.Error(err)
			debug.PrintStack()
		}
	}()

	if merged {
		mergeWorker := bitcask.NewMerge(bc, inteval)
		mergeWorker.Start()
		defer mergeWorker.Stop()
	}

	r := mux.NewRouter()
	r.HandleFunc("/{key}", bitcaskGetHandle).Methods("GET")
	r.HandleFunc("/{key}", bitcaskDelHandle).Methods("DELETE")
	r.HandleFunc("/{key}", bitcaskPutHandle).Methods("POST")
	logger.Info("bitcask server listen:", addr)
	if err := http.ListenAndServe(addr, r); err != nil {
		logger.Error(err)
	}
}

func bitcaskGetHandle(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	key := vars["key"]
	if len(key) <= 0 {
		w.Write([]byte("key invalid"))
		return
	}

	value, err := bc.Get([]byte(key))
	if err != nil && err != bitcask.ErrNotFound {
		logger.Error(err)
		w.WriteHeader(500)
		return
	}

	if err == bitcask.ErrNotFound {
		w.WriteHeader(404)
		w.Write([]byte(bitcask.ErrNotFound.Error()))
		return
	}

	w.Write(value)
}

func bitcaskPutHandle(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	key := vars["key"]
	if len(key) <= 0 {
		w.Write([]byte("key invalid"))
		return
	}
	value, err := ioutil.ReadAll(r.Body)
	if err != nil {
		logger.Error(err)
		w.WriteHeader(500)
		return
	}

	bc.Put([]byte(key), value)
	w.Write([]byte("Success"))
}

func bitcaskDelHandle(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	key := vars["key"]
	if len(key) <= 0 {
		w.Write([]byte("key invalid"))
		return
	}

	err := bc.Del([]byte(key))
	if err != nil && err != bitcask.ErrNotFound {
		logger.Error(err)
		w.WriteHeader(500)
		return
	}
	if err == bitcask.ErrNotFound {
		w.WriteHeader(404)
		w.Write([]byte(bitcask.ErrNotFound.Error()))
		return
	}

	w.Write([]byte("Success"))
}
