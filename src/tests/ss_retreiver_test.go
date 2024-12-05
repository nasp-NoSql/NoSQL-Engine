package tests

import (
	"nosqlEngine/src/models/key_value"
	"nosqlEngine/src/service"
	"testing"
)

type FileReaderMock struct {
	rawBytes []byte
}

func (fw *FileReaderMock) ReadBlock(size int) ([]byte, error) {
	return fw.rawBytes, nil
}

func TestRetrieveAKey(t *testing.T) {
	fileWriterMock := &FileWriterMock{}
	ssParser := service.NewSSParser1File(fileWriterMock)

	keyValues := make([]key_value.KeyValue, 0, 3)
	for i := 0; i < 3; i++ {
		keyValues = append(keyValues, key_value.NewKeyValue("key", "value"))
	}

	ssParser.AddMemtable(keyValues)

	raw := fileWriterMock.rawBytes
	reader := service.NewSSTableReader(raw)

	value, err := reader.GetValue("key")
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if value != "value" {
		t.Fatalf("expected value, got %v", value)
	}
}