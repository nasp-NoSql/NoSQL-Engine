package ss_parser

import (
	"encoding/binary"
	"fmt"
	"nosqlEngine/src/config"
	"nosqlEngine/src/models/key_value"
	"nosqlEngine/src/service/file_writer"
)

var CONFIG = config.GetConfig()

func SerializeDataGetOffsets(fw file_writer.FileWriterInterface, keyValues []key_value.KeyValue) ([]string, []int) {
	keys := []string{}
	offsets := []int{}
	currBlockIndex := -1
	for i := 0; i < len(keyValues); i++ {
		value := append(SizeAndValueToBytes(keyValues[i].GetKey()), SizeAndValueToBytes(keyValues[i].GetValue())...)
		blockIndex := fw.Write(value, false)
		fmt.Print("Writing on the index: ", i, " with block index: ", blockIndex, "\n")
		if currBlockIndex != blockIndex {
			currBlockIndex = blockIndex
			keys = append(keys, keyValues[i].GetKey())
			offsets = append(offsets, currBlockIndex)
		}
	}
	return keys, offsets
}

func SerializeIndexGetOffsets(keys []string, keyOffsets []int, fw file_writer.FileWriterInterface) []int {
	fmt.Print("Serializing index\n")
	fmt.Print("Number of keys: ", len(keys), "\n")
	fmt.Print("Number of key offsets: ", len(keyOffsets), "\n")
	for i := 0; i < len(keys); i++ {
		fmt.Print("Key: ", keys[i], "\n")
	}
	blockIndex := []int{}
	for i := 0; i < len(keys); i++ {
		value := append(SizeAndValueToBytes(keys[i]), IntToBytes(int64(keyOffsets[i]))...)
		currBlock := fw.Write(value, false)
		blockIndex = append(blockIndex, currBlock)
	}
	return blockIndex
}
func SerializeSummary(keys []string, offsets []int, fw file_writer.FileWriterInterface) {
	fmt.Print("Serializing summary\n")
	for i := 0; i < len(keys); i = i + CONFIG.SummaryStep {
		value := append(SizeAndValueToBytes(keys[i]), IntToBytes(int64(offsets[i]))...)
		fw.Write(value, false)
	}
}

func SerializeMetaData(summaryStartOffset int, bloomFilterBytes []byte, merkleTreeBytes []byte, numOfItems int, fw file_writer.FileWriterInterface) {
	fmt.Print("serializing metadata\n")
	fw.Write(IntToBytes(int64(len(bloomFilterBytes))), false)
	fmt.Printf("Bloom filter size: %d\n", len(bloomFilterBytes))
	fw.Write(bloomFilterBytes, false)
	fmt.Printf("Bloom filter data: %v\n", bloomFilterBytes)
	fw.Write(IntToBytes(int64(summaryStartOffset)), false)
	fmt.Printf("Summary start: %d\n", summaryStartOffset)
	fw.Write(IntToBytes((int64(numOfItems))), false)
	fmt.Printf("Number of items: %d\n", numOfItems)
	fw.Write(merkleTreeBytes, true)
	fmt.Printf("Merkle tree data: %v\n", merkleTreeBytes)
	metadataLength := 8 + len(bloomFilterBytes) + 8 + 8 + len(merkleTreeBytes)
	fmt.Printf("Metadata length: %d\n", metadataLength)
	fw.Write(IntToBytes(int64(metadataLength)), true) // Write metadata length
}

func IntToBytes(n int64) []byte {
	buf := make([]byte, 8) // 8 bytes for int64
	binary.BigEndian.PutUint64(buf, uint64(n))
	return buf
}

func SizeAndValueToBytes(value string) []byte {
	valueBytes := []byte(value)
	valueSizeBytes := IntToBytes(int64(len(valueBytes)))
	return append(valueSizeBytes, valueBytes...)
}
