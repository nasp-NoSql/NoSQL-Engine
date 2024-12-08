package tests

import (
	"fmt"
	"nosqlEngine/src/models/key_value"
	"nosqlEngine/src/service"
	"nosqlEngine/src/service/block_manager"
	"nosqlEngine/src/service/file_reader"
	"nosqlEngine/src/service/file_writer"
	"nosqlEngine/src/service/ss_parser"
	"testing"
)

type FileReaderMock struct {
	rawBytes []byte
}

func (fw *FileReaderMock) ReadBlock(size int) ([]byte, error) {
	return fw.rawBytes, nil
}

func TestWriteToSS(t *testing.T) {
	bm := block_manager.NewManager(30, 5)
	fileWriter1File := file_writer.NewFileWriter1File(bm)
	ssParser := ss_parser.NewSSParser1File(fileWriter1File)

	keyValues := make([]key_value.KeyValue, 0, 3)
	for i := 0; i < 3; i++ {
		key := fmt.Sprintf("key%d", i+1)
		value := fmt.Sprintf("value%d", i+1)
		keyValues = append(keyValues, key_value.NewKeyValue(key, value))
	}

	ssParser.AddMemtable(keyValues)
}

func TestRetrieveAKey(t *testing.T) {
	bm := block_manager.NewManager(30, 5)
	fileWriter1File := file_writer.NewFileWriter1File(bm)
	ssParser := ss_parser.NewSSParser1File(fileWriter1File)

	keyValues := make([]key_value.KeyValue, 0, 3)
	for i := 0; i < 3; i++ {
		key := fmt.Sprintf("key%d", i+1)
		value := fmt.Sprintf("value%d", i+1)
		keyValues = append(keyValues, key_value.NewKeyValue(key, value))
	}

	ssParser.AddMemtable(keyValues)

	reader := file_reader.NewReader(bm)
	retriever := service.NewSSRetriever(reader)

	value, err := retriever.GetValue("key")
	if err != nil {
		t.Errorf("Error retrieving key: %s", err)
	}

	fmt.Println(value)
}
