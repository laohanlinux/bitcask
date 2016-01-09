package bitcask

import "os"

func lockFile(fileName string) (*os.File, error) {
	return os.OpenFile(fileName, os.O_WRONLY|os.O_CREATE, os.ModePerm)
}
