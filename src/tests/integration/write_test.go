package integration

import (
	"encoding/binary"
	"fmt"
	"nosqlEngine/src/models/key_value"
	r "nosqlEngine/src/service"
	b "nosqlEngine/src/service/block_manager"
	rw "nosqlEngine/src/service/file_reader"
	fw "nosqlEngine/src/service/file_writer"
	"nosqlEngine/src/service/ss_parser"
	"testing"
)

func bytesToInt(buf []byte) int64 {

	return int64(binary.BigEndian.Uint64(buf))
}
func TestWritePathIntegration(t *testing.T) {
	// Setup block manager and file writer
	bm := b.NewBlockManager()
	blockSize := b.BLOCK_SIZE
	fileWriter := fw.NewFileWriter(*bm, blockSize)
	ssParser := ss_parser.NewSSParser1File(*fileWriter)

	// Create a set of key-value pairs
	keyValues := make([]key_value.KeyValue, 0, 10)
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("key%d", i+1)
		value := fmt.Sprintf("value%d", i+1)
		keyValues = append(keyValues, key_value.NewKeyValue(key, value))
	}

	// Write the memtable to disk via the parser and file writer
	ssParser.AddMemtable(keyValues)

	// Read the file to verify the data
	data, err := bm.ReadBlock(fileWriter.GetLocation(), 0, true)
	if err != nil {
		t.Fatalf("Failed to read block: %v", err)
	}

	//check if the block data matches the expected serialized data
	expectedKey := "key1"
	expectedValue := "value1"

	keySize := bytesToInt(data[:8])
	valueSize := bytesToInt(data[8+keySize : 16+keySize])

	if string(data[8:8+keySize]) != expectedKey {
		t.Errorf("Key mismatch: got %s, want %s", data[8:8+keySize], expectedKey)
	}
	if string(data[16+keySize:16+keySize+valueSize]) != expectedValue {
		t.Errorf("Value mismatch: got %s, want %s", data[16+keySize:16+keySize+valueSize], expectedValue)
	}
}

func TestWriteRead(t *testing.T) {
	bm := b.NewBlockManager()
	blockSize := b.BLOCK_SIZE
	fileWriter := fw.NewFileWriter(*bm, blockSize)
	ssParser := ss_parser.NewSSParser1File(*fileWriter)

	// Create a set of key-value pairs
	keyValues := make([]key_value.KeyValue, 0, 10)
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("key%d", i+1)
		value := fmt.Sprintf("value%d", i+1)
		keyValues = append(keyValues, key_value.NewKeyValue(key, value))
	}

	// Write the memtable to disk via the parser and file writer
	ssParser.AddMemtable(keyValues)

	reader := rw.NewFileReader(fileWriter.GetLocation(), blockSize, *bm)

	retriever := r.NewEntryRetriever(*reader)

	retrievedData, err := retriever.RetrieveEntry("key1")

	//this is only metadata

	if err != nil {
		t.Fatalf("Failed to retrieve entry: %v", err)
	}

	//in tis format 	data = append(data, bf_data...)
	// data = append(data, intToBytes(summary_start)...)
	// data = append(data, intToBytes(summary_size)...)
	// data = append(data, intToBytes(numOfItems)...)
	// data = append(data, merkle_data...)
	// data = append(data, intToBytes(merkle_size)...)
	// data = append(data, cleanedData[:len(cleanedData)-40-bf_size_int-merkle_size_int]...)

	if len(retrievedData) == 0 {
		t.Errorf("Retrieved data is empty")
	}
	for i := 0; i < len(retrievedData); i++ {
		fmt.Printf("Retrieved data[%d]: %d\n", i, retrievedData[i])
	}

}
