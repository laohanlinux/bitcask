package bitcask

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
)

// ErrCrc32 ...
var ErrCrc32 = fmt.Errorf("file is dirty")

func encodeEntry(tStamp, keySize, valueSize uint32, key, value []byte) []byte {
	/**
	    crc32	:	tStamp	:	ksz	:	valueSz	:	key	:	value
	    4 		:	4 		: 	4 	: 		4	:	xxxx	: xxxx
	**/
	bufSize := 16 + keySize + valueSize
	buf := make([]byte, bufSize)
	binary.LittleEndian.PutUint32(buf[4:8], tStamp)
	binary.LittleEndian.PutUint32(buf[8:12], keySize)
	binary.LittleEndian.PutUint32(buf[12:16], valueSize)
	copy(buf[16:(16+keySize)], key)
	copy(buf[(16+keySize):(16+keySize+valueSize)], value)
	c32 := crc32.ChecksumIEEE(buf[4:])
	binary.LittleEndian.PutUint32(buf[0:4], uint32(c32))
	return buf
}

func decodeEntry(buf []byte) ([]byte, error) {
	/**
	    crc32	:	tStamp	:	ksz	:	valueSz	:	key	:	value
	    4 		:	4 		: 	4 	: 		4	:	xxxx	: xxxx
	**/
	ksz := binary.LittleEndian.Uint32(buf[8:12])

	valuesz := binary.LittleEndian.Uint32(buf[12:16])
	c32 := binary.LittleEndian.Uint32(buf[:4])
	value := make([]byte, valuesz)
	copy(value, buf[(16+ksz):(16+ksz+valuesz)])

	if crc32.ChecksumIEEE(buf[4:]) != c32 {
		return nil, ErrCrc32
	}
	return value, nil
}

func encodeHint(tStamp, ksz, valueSz uint32, valuePos uint64, key []byte) []byte {
	/**
		    tStamp	:	ksz	:	valueSz	:	valuePos	:	key
	        4       :   4   :   4       :       8       :   xxxxx
	**/
	buf := make([]byte, 20+len(key))
	binary.LittleEndian.PutUint32(buf[0:4], tStamp)
	binary.LittleEndian.PutUint32(buf[4:8], ksz)
	binary.LittleEndian.PutUint32(buf[8:12], valueSz)
	binary.LittleEndian.PutUint64(buf[12:20], valuePos)
	copy(buf[20:], []byte(key))
	return buf
}

func decodeHint(buf []byte) (uint32, uint32, uint32, uint64) {
	/**
	    tStamp	:	ksz	:	valueSz	:	valuePos	:	key
		4       :   4   :   4       :       8       :   xxxxx
	**/
	tStamp := binary.LittleEndian.Uint32(buf[:4])
	ksz := binary.LittleEndian.Uint32(buf[4:8])
	valueSz := binary.LittleEndian.Uint32(buf[8:12])
	valuePos := binary.LittleEndian.Uint64(buf[12:20])
	return tStamp, ksz, valueSz, valuePos
}
