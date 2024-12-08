package tests

import (
	"fmt"
	"nosqlEngine/src/models/key_value"
	"nosqlEngine/src/service/ss_parser"
	"testing"
)

type FileWriterMock struct {
	rawBytes []byte
}

func (fw *FileWriterMock) WriteSS(data ...[]byte) bool {
	fw.rawBytes = data[0]
	return true
}

func TestAddMemtable(t *testing.T) {
	fileWriterMock := &FileWriterMock{}
	ssParser := ss_parser.NewSSParser1File(fileWriterMock)

	keyValues := make([]key_value.KeyValue, 0, 3)
	for i := 0; i < 3; i++ {
		key := fmt.Sprintf("key%d", i+1)
		value := fmt.Sprintf("value%d", i+1)
		keyValues = append(keyValues, key_value.NewKeyValue(key, value))
	}
	ssParser.AddMemtable(keyValues)

	raw := fileWriterMock.rawBytes

	if len(raw) == 0 {
		t.Error("Expected raw bytes to be written to FileWriterMock, got 0 bytes")
	}
	fmt.Printf("Raw bytes: %v\n", raw)
	fmt.Print("Length of raw bytes: ", len(raw), "\n")
}
