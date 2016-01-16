package bitcask

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"testing"
	"time"

	"github.com/laohanlinux/assert"
)

func TestEncodeDecodeEntry(t *testing.T) {
	/**
		crc32	:	tStamp	:	ksz	:	valueSz	:	key	:	value
		4 		:	4 		: 	4 	: 		4	:	xxxx	: xxxx
	**/
	//func encodeEntry(tStamp, keySize, valueSize uint32, key, value []byte) []byte {
	// EncodeEntry
	tStamp := uint32(time.Now().Unix())
	key := []byte("Foo")
	value := []byte("Bar")
	ksz := uint32(len(key))
	valuesz := uint32(len(value))
	buf := make([]byte, HeaderSize+ksz+valuesz)
	binary.LittleEndian.PutUint32(buf[4:8], tStamp)
	binary.LittleEndian.PutUint32(buf[8:12], ksz)
	binary.LittleEndian.PutUint32(buf[12:16], valuesz)
	copy(buf[16:(16+ksz)], key)
	copy(buf[(16+ksz):(16+ksz+valuesz)], value)
	c32 := crc32.ChecksumIEEE(buf[4:])
	binary.LittleEndian.PutUint32(buf[0:4], uint32(c32))
	// Test decode

	ksz = binary.LittleEndian.Uint32(buf[8:12])
	valuesz = binary.LittleEndian.Uint32(buf[12:16])
	tStamp = binary.LittleEndian.Uint32(buf[4:8])
	c32 = binary.LittleEndian.Uint32(buf[:4])
	assert.Equal(t, binary.LittleEndian.Uint32(buf[0:4]), c32)
	assert.Equal(t, binary.LittleEndian.Uint32(buf[4:8]), tStamp)
	assert.Equal(t, binary.LittleEndian.Uint32(buf[8:12]), ksz)
	assert.Equal(t, binary.LittleEndian.Uint32(buf[12:16]), valuesz)
	assert.Equal(t, buf[HeaderSize:(HeaderSize+ksz)], key)
	assert.Equal(t, buf[(HeaderSize+ksz):(HeaderSize+ksz+valuesz)], value)

	// EncodeEntry , ksz = 0, valueSz = 0
	ksz = uint32(0)
	valuesz = uint32(0)
	buf = make([]byte, HeaderSize+ksz+valuesz, HeaderSize+ksz+valuesz)
	binary.LittleEndian.PutUint32(buf[4:8], tStamp)
	binary.LittleEndian.PutUint32(buf[8:12], ksz)
	binary.LittleEndian.PutUint32(buf[12:16], valuesz)
	c32 = crc32.ChecksumIEEE(buf[4:])
	binary.LittleEndian.PutUint32(buf[0:4], c32)
	// decodeEntry, ksz =0, valueSz = 0
	assert.Equal(t, binary.LittleEndian.Uint32(buf[0:4]), c32)
	assert.Equal(t, binary.LittleEndian.Uint32(buf[4:8]), tStamp)
	assert.Equal(t, binary.LittleEndian.Uint32(buf[8:12]), ksz)
	assert.Equal(t, binary.LittleEndian.Uint32(buf[12:16]), valuesz)
}

func TestEncodeDecodeHint(t *testing.T) {
	/**
			tStamp	:	ksz	:	valueSz	:	valuePos	:	key
			4       :   4   :   4       :       8       :   xxxxx
	**/
	// encodeHint
	tStamp := uint32(time.Now().Unix())
	key := []byte("Foo")
	value := []byte("Bar")
	ksz := uint32(len(key))
	valuesz := uint32(len(value))
	valuePos := uint64(8)
	buf := make([]byte, HintHeaderSize+ksz, HintHeaderSize+ksz)
	binary.LittleEndian.PutUint32(buf[0:4], tStamp)
	binary.LittleEndian.PutUint32(buf[4:8], ksz)
	binary.LittleEndian.PutUint32(buf[8:12], valuesz)
	binary.LittleEndian.PutUint64(buf[12:20], valuePos)
	copy(buf[HintHeaderSize:], key)
	// decodeHint
	assert.Equal(t, binary.LittleEndian.Uint32(buf[:4]), tStamp)
	assert.Equal(t, binary.LittleEndian.Uint32(buf[4:8]), ksz)
	assert.Equal(t, binary.LittleEndian.Uint32(buf[8:12]), valuesz)
	assert.Equal(t, binary.LittleEndian.Uint64(buf[12:20]), valuePos)
	fmt.Println(string(buf[HintHeaderSize : HintHeaderSize+ksz]))
	assert.Equal(t, buf[HintHeaderSize:], key)

	ksz = 0
	valuesz = 0
	valuePos = 0
	buf = make([]byte, HintHeaderSize+ksz, HintHeaderSize+ksz)
	binary.LittleEndian.PutUint32(buf[0:4], tStamp)
	binary.LittleEndian.PutUint32(buf[4:8], ksz)
	binary.LittleEndian.PutUint32(buf[8:12], valuesz)
	binary.LittleEndian.PutUint64(buf[12:20], valuePos)
	assert.Equal(t, binary.LittleEndian.Uint32(buf[:4]), tStamp)
	assert.Equal(t, binary.LittleEndian.Uint32(buf[4:8]), ksz)
	assert.Equal(t, binary.LittleEndian.Uint32(buf[8:12]), valuesz)
	assert.Equal(t, binary.LittleEndian.Uint64(buf[12:20]), valuePos)
}
