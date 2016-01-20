package main

import (
	"fmt"
	"io"
	"os"
	"time"

	"github.com/laohanlinux/bitcask"
	"github.com/laohanlinux/go-logger/logger"
)

func main() {
	d1()
}

func h1() {
	buf := make([]byte, bitcask.HintHeaderSize)
	fmt.Println("hfp:", os.Args[1], "dfp:", os.Args[2])
	fp, err := os.Open(os.Args[1])
	dfp, _ := os.Open(os.Args[2])
	if err != nil {
		logger.Fatal(err)
	}

	for {
		n, err := fp.Read(buf)
		if err != nil && err != io.EOF {
			logger.Fatal(err)
		}

		if err == io.EOF {
			break
		}

		if n != len(buf) || n != bitcask.HintHeaderSize {
			logger.Fatal(n)
		}

		htStamp, hksz, hvaluesz, hvaluePos := bitcask.DecodeHint(buf)
		logger.Debug("hintSize:", hksz)
		time.Sleep(time.Second * 3)
		key := make([]byte, hksz)
		fp.Read(key)
		fmt.Println("Hint:", "key:", string(key), htStamp, "ksz:", hksz, "valuesize:", hvaluesz, "pos:", hvaluePos)
		if err != nil {
			logger.Fatal(err)
		}
		// read
		dbuf := make([]byte, bitcask.HeaderSize+hksz+hvaluesz)
		dfp.ReadAt(dbuf, int64(hvaluePos))
		dvalue, err := bitcask.DecodeEntry(dbuf)
		if err != nil {
			logger.Fatal(err)
		}
		fmt.Println("dvalue:", string(dvalue))
		os.Exit(0)
	}
}

func d1() {
	buf := make([]byte, bitcask.HeaderSize)
	fp, err := os.Open(os.Args[1])
	if err != nil {
		logger.Fatal(err)
	}

	offset := int64(0)
	for {
		n, err := fp.ReadAt(buf, offset)
		if err != nil && err != io.EOF {
			logger.Fatal(err)
		}
		if err == io.EOF {
			break
		}
		if n != len(buf) || n != bitcask.HeaderSize {
			logger.Fatal(n)
		}
		offset += int64(n)
		// parse data header
		c32, tStamp, ksz, valuesz := bitcask.DecodeEntryHeader(buf)
		logger.Info(c32, tStamp, "ksz:", ksz, "valuesz:", valuesz)
		if err != nil {
			logger.Fatal(err)
		}

		if ksz+valuesz == 0 {
			continue
		}

		keyValue := make([]byte, ksz+valuesz)
		n, err = fp.ReadAt(keyValue, offset)
		if err != nil && err != io.EOF {
			logger.Fatal(err)
		}
		if err == io.EOF {
			break
		}
		offset += int64(n)
		fmt.Println(string(keyValue[:ksz]), string(keyValue[ksz:]))
	}
}

func d2() {
	buf := make([]byte, bitcask.HeaderSize)
	fp, err := os.Open(os.Args[1])
	if err != nil {
		logger.Fatal(err)
	}

	for {
		n, err := fp.Read(buf[0:])
		if err != nil && err != io.EOF {
			logger.Fatal(err)
		}
		if n != len(buf) {
			logger.Fatal(n)
		}
		value, err := bitcask.DecodeEntry(buf)
		logger.Info(value)
		if err != nil {
			logger.Fatal(err)
		}
		//logger.Info(c32, tStamp, ksz, valuesz, key, value)
	}
}
