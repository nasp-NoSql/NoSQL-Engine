package tests

import (
	"testing"
)

type FileReaderMock struct {
	rawBytes []byte
}

func (fw *FileReaderMock) ReadBlock(size int) ([]byte, error) {
	return fw.rawBytes, nil
}

func TestWriteToSS(t *testing.T) {

}

func TestRetrieveAKey(t *testing.T) {

}
