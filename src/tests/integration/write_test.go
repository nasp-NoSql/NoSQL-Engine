package integration

import (
	"encoding/binary"
	"fmt"
	"nosqlEngine/src/config"
	r "nosqlEngine/src/service"
	b "nosqlEngine/src/service/block_manager"
	fw "nosqlEngine/src/service/file_writer"
	"nosqlEngine/src/service/ss_parser"
	m "nosqlEngine/src/storage/memtable"
	"testing"

	"github.com/google/uuid"
)

var CONFIG = config.GetConfig()

func bytesToInt(buf []byte) int64 {

	return int64(binary.BigEndian.Uint64(buf))
}
func TestWritePathIntegration(t *testing.T) {
	// Setup block manager and file writer
	bm := b.NewBlockManager()
	blockSize := CONFIG.BlockSize
	fileWriter := fw.NewFileWriter(bm, blockSize, "sstable/sstable_"+uuid.New().String()+".db")
	ssParser := ss_parser.NewSSParser(fileWriter)
	mt := m.NewMemtable()

	// Create a set of key-value pairs
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("key%d", i+1)
		value := fmt.Sprintf("value%d", i+1)
		mt.Add(key, value)
	}

	// Write the memtable to disk via the parser and file writer
	ssParser.FlushMemtable(mt.ToRaw())

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
	mt := m.NewMemtable()
	bm := b.NewBlockManager()
	blockSize := CONFIG.BlockSize
	uuidStr := uuid.New().String()

	fileWriter := fw.NewFileWriter(bm, blockSize, "sstable/sstable_"+uuidStr+".db")
	ssParser := ss_parser.NewSSParser(fileWriter)

	// Create a set of key-value pairs
	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("key%d", i+1)

		value := fmt.Sprintf("value%d", i+1)
		mt.Add(key, value)

	}

	// Write the memtable to disk via the parser and file writer
	ssParser.FlushMemtable(mt.ToRaw())
	fmt.Print(
		"File written successfully, now reading the data back...\n")

	retriever := r.NewEntryRetriever(bm)

	res, err := retriever.RetrieveEntry("key167")

	if err != nil {
		t.Fatalf("Failed to retrieve entry: %v for metadata: %v", err, res)
	}

}
