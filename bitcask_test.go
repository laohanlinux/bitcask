package bitcask

import (
	"fmt"
	"testing"

	"github.com/laohanlinux/assert"
)

func TestBitCask(t *testing.T) {
	// clear dirty
	//os.RemoveAll("testBitcask")
	b, err := Open("testBitcask", nil)
	fmt.Println(err)
	assert.Nil(t, err)
	assert.NotNil(t, b)

	testKey := []byte("Foo")
	value := []byte("Bar")
	b.Put(testKey, value)
	v, err := b.Get(testKey)
	assert.Nil(t, err)
	fmt.Println("value:", string(v))
	assert.Equal(t, v, value)

	testKey = []byte("xiaoMing")
	value = []byte("住在棠下")
	b.Put(testKey, value)
	v, err = b.Get(testKey)
	fmt.Println("value:", string(v))
	assert.Equal(t, v, value)

	value = []byte("住在学院路")
	b.Put(testKey, value)
	v, err = b.Get(testKey)
	fmt.Println("value:", string(v))
	assert.Equal(t, v, value)

	b.Close()
}
