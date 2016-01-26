package bitcask

import (
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/laohanlinux/assert"
	"github.com/laohanlinux/go-logger/logger"
)

// rebuild file test
func TestSplit1(t *testing.T) {
	storagePath := "split1Bitcask"
	os.RemoveAll(storagePath)
	bc, err := Open(storagePath, nil)
	assert.Nil(t, err)
	testKey := []byte("Foo")

	value := []byte("Bar")
	bc.Put(testKey, value)
	v, err := bc.Get(testKey)
	assert.Nil(t, err)
	assert.Equal(t, v, value)

	bc.Close()

	storagePath = "split1Bitcask"
	bc, err = Open(storagePath, nil)
	assert.Nil(t, err)
	testKey = []byte("Foo")

	value = []byte("Bar")
	bc.Put(testKey, value)
	v, err = bc.Get(testKey)
	assert.Nil(t, err)
	assert.Equal(t, v, value)
	bc.Close()
}

func TestSplit2(t *testing.T) {
	storagePath := "split2Bitcask"
	os.RemoveAll(storagePath)
	opts := &Options{
		MaxFileSize: 2,
	}
	bc, err := Open(storagePath, opts)
	assert.Nil(t, err)
	testKey := []byte("Foo")

	value := []byte("Bar")
	bc.Put(testKey, value)
	v, err := bc.Get(testKey)
	assert.Nil(t, err)
	assert.Equal(t, v, value)
	logger.Info("==============================")
	time.Sleep(time.Second * 2)

	// cause split file
	value = []byte("Apple")
	bc.Put(testKey, value)
	v, err = bc.Get(testKey)
	assert.Nil(t, err)
	assert.Equal(t, v, value)
	bc.Close()
}

func TestBitCask(t *testing.T) {
	// clear dirty
	os.RemoveAll("testBitcask")
	b, err := Open("testBitcask", nil)
	logger.Info(err)
	assert.Nil(t, err)
	assert.NotNil(t, b)

	testKey := []byte("Foo")
	value := []byte("Bar")
	b.Put(testKey, value)
	v, err := b.Get(testKey)
	assert.Nil(t, err)
	logger.Info("value:", string(v))
	assert.Equal(t, v, value)

	testKey = []byte("xiaoMing")
	value = []byte("abc")
	b.Put(testKey, value)
	v, err = b.Get(testKey)
	logger.Info("value:", string(v))
	assert.Equal(t, v, value)

	// hintFile:
	value = []byte("ddddd")
	b.Put(testKey, value)
	v, err = b.Get(testKey)
	logger.Info("value:", string(v))
	assert.Equal(t, v, value)

	b.Close()
}

func BenchmarkBitcaskCurrency(b *testing.B) {
	storagePath := "benchMarkBitcask"
	os.RemoveAll(storagePath)
	opts := &Options{
		MaxFileSize: 1 << 12,
	}
	bc, err := Open(storagePath, opts)
	if err != nil {
		logger.Fatal(err)
	}

	keyValues := make(map[int]string)

	for i := 0; i < b.N/2; i++ {
		key := strconv.Itoa(i)
		value := strconv.Itoa(int(time.Now().Unix()))
		bc.Put([]byte(key), []byte(value))
		keyValues[i] = value
	}
	logger.Warn(b.N)
	logger.Info("Put all Data")
	for i := 0; i < b.N/2; i++ {
		k := strconv.Itoa(i)
		v, _ := bc.Get([]byte(k))
		if string(v) != keyValues[i] {
			logger.Error(string(v), keyValues[i])
			os.Exit(-1)
		}
	}
	logger.Info("Get all data")
	// delete all data
	for i := 0; i < b.N/2; i++ {
		k := strconv.Itoa(i)
		//v, _ := bc.Get([]byte(k))
		err := bc.Del([]byte(k))
		if err != nil {
			logger.Error(err)
		}
	}
	logger.Info("Delete all data")
	// Get all data
	for i := 0; i < b.N/2; i++ {
		k := strconv.Itoa(i)
		v, err := bc.Get([]byte(k))
		if err != ErrNotFound {
			logger.Error(string(v), keyValues[i])
		}
	}
	logger.Info("all data is not found, pass test")
	//mergeWorker.Staop()
	bc.Close()
}
