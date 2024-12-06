package tests

import (
	"nosqlEngine/src/models/key_value"
	"nosqlEngine/src/service"
	"testing"

	"nosqlEngine/src/service/block_manager"
)

type FileReaderMock struct {
	rawBytes []byte
}

func (fw *FileReaderMock) ReadBlock(size int) ([]byte, error) {
	return fw.rawBytes, nil
}

func TestWriteToSS(t *testing.T) {
	bm := block_manager.NewManager(30, 5)
	fileWriter1File := service.NewFileWriter1File(bm)
	ssParser := service.NewSSParser1File(fileWriter1File)

	keyValues := make([]key_value.KeyValue, 0, 3)
	for i := 0; i < 3; i++ {
		keyValues = append(keyValues, key_value.NewKeyValue("key", "value"))
	}

	ssParser.AddMemtable(keyValues)
}
