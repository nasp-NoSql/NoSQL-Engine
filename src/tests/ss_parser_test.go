package tests

import (
	"fmt"
	"nosqlEngine/src/models/key_value"
	"nosqlEngine/src/service/file_writer"
	"nosqlEngine/src/service/ss_parser"
	"testing"
)

// other imports...

// FileWriterMock implements the same interface as file_writer.FileWriter
type FileWriterMock struct {
	rawBytes []byte
	blockNum int
}

type FileWriterInterface interface {
	Write(data []byte, sectionEnd bool) int //implementation of the method from file_writer.FileWriterInterface
}

var _ file_writer.FileWriterInterface = (*FileWriterMock)(nil) // Ensure FileWriterMock implements the interface

func (fw *FileWriterMock) Write(data []byte, sectionEnd bool) int {
	fw.rawBytes = append(fw.rawBytes, data...)
	if sectionEnd {
		fw.rawBytes = append(fw.rawBytes, 0) // Append a section end marker
	}
	return fw.blockNum
}

func TestAddMemtable(t *testing.T) {
	fileWriterMock := &FileWriterMock{}
	ssParser := ss_parser.NewSSParser(fileWriterMock)

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
