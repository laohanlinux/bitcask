package bitcask

import "os"

func lockFile(fileName string) (*os.File, error) {
	return os.OpenFile(fileName, os.O_WRONLY|os.O_CREATE, os.ModePerm)
}

func lockAcquire(fileName string, isWriteLock bool) *Lock {

	var fp *os.File
	var err error
	if isWriteLock {
		if fp, err = lockFile(fileName); err != nil {
			panic(err)
		}
	}

	return &Lock{
		Fp:          fp,
		IsWriteLock: isWriteLock,
	}
}
