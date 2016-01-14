package bitcask

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"testing"
	"time"
)

func TestEnBin(t *testing.T) {
	/**
		crc32	:	tStamp	:	ksz	:	valueSz	:	key	:	value
		4 		:	4 		: 	4 	: 		4	:	xxxx	: xxxx
	**/
	//	crc32 := int32(1000)
	tStamp := uint32(time.Now().Unix())
	Key := []byte("Foo")
	value := []byte("Bar")
	ksz := uint32(len(Key))
	valuesz := uint32(len(value))
	buflen := (16 + ksz + valuesz)

	buf := make([]byte, buflen)

	binary.LittleEndian.PutUint32(buf[4:8], uint32(tStamp))
	binary.LittleEndian.PutUint32(buf[8:12], uint32(ksz))
	binary.LittleEndian.PutUint32(buf[12:16], uint32(valuesz))
	copy(buf[16:(16+ksz)], Key)
	copy(buf[(16+ksz):(16+ksz+valuesz)], value)

	// cre32
	c32 := crc32.ChecksumIEEE(buf[4:])
	binary.LittleEndian.PutUint32(buf[0:4], uint32(c32))

	fmt.Printf("%s\n", buf)
	// decode
	ksz = binary.LittleEndian.Uint32(buf[8:12])
	valuesz = binary.LittleEndian.Uint32(buf[12:16])
	tStamp = binary.LittleEndian.Uint32(buf[4:8])
	c32 = binary.LittleEndian.Uint32(buf[:4])
	Key = make([]byte, ksz)
	value = make([]byte, valuesz)
	copy(Key, buf[16:(16+ksz)])
	copy(value, buf[(16+ksz):(16+ksz+valuesz)])
	fmt.Println(ksz, valuesz, tStamp)
	fmt.Printf("Key:%s\nvalue:%s\n", Key, value)
	if crc32.ChecksumIEEE(buf[4:]) == c32 {
		fmt.Println("crc32 true")
	} else {
		fmt.Println("crc32 fals")
	}
}
