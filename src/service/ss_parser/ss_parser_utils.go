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
	fmt.Print("Serializing data...\n")
	keys := []string{}
	offsets := []int{}
	currBlockIndex := -1
	for i := 0; i < len(keyValues); i++ {
		value := append(SizeAndValueToBytes(keyValues[i].GetKey()), SizeAndValueToBytes(keyValues[i].GetValue())...)
		blockIndex := fw.Write(value, false, nil)
		if currBlockIndex != blockIndex {
			currBlockIndex = blockIndex
			keys = append(keys, keyValues[i].GetKey())
			offsets = append(offsets, currBlockIndex)
		}
	}
	return keys, offsets
}

func SerializeIndexGetOffsets(keys []string, keyOffsets []int, fw file_writer.FileWriterInterface) []int {
	fmt.Print("Serializing index...\n")
	blockIndex := []int{}
	for i := 0; i < len(keys); i++ {
		value := append(SizeAndValueToBytes(keys[i]), IntToBytes(int64(keyOffsets[i]))...)
		currBlock := fw.Write(value, false, nil)
		blockIndex = append(blockIndex, currBlock)
	}
	return blockIndex
}
func SerializeSummary(keys []string, offsets []int, fw file_writer.FileWriterInterface) {
	fmt.Print("Serializing summary...\n")
	for i := 0; i < len(keys); i = i + CONFIG.SummaryStep {
		value := append(SizeAndValueToBytes(keys[i]), IntToBytes(int64(offsets[i]))...)
		fw.Write(value, false, nil)
	}
}

func SerializeMetaData(summaryStartOffset int, bloomFilterBytes []byte, merkleTreeBytes []byte, numOfItems int, fw file_writer.FileWriterInterface) {
	fmt.Print("Serializing metadata...\n")
	fw.Write(IntToBytes(int64(len(bloomFilterBytes))), false, nil)
	fmt.Printf("Bloom filter bytes length: %d\n", len(IntToBytes(int64(len(bloomFilterBytes)))))
	fw.Write(bloomFilterBytes, false, nil)
	fmt.Printf("Bloom filter bytes record len: %v\n", len(bloomFilterBytes))
	fw.Write(IntToBytes(int64(summaryStartOffset)), false, nil)
	fmt.Printf("Summary start offset record len: %d\n", len(IntToBytes(int64(summaryStartOffset))))
	fw.Write(IntToBytes(int64(numOfItems)), false, nil)
	fmt.Printf("Number of items record len: %d\n", len(IntToBytes(int64(numOfItems))))
	fw.Write(IntToBytes(int64(len(merkleTreeBytes))), false, nil)
	fmt.Printf("Merkle tree bytes length: %d\n", len(IntToBytes(int64(len(merkleTreeBytes)))))
	fw.Write(merkleTreeBytes, false, nil)
	fmt.Printf("Merkle tree bytes record len: %v\n", len(merkleTreeBytes))
	metadataLength := 8 + len(bloomFilterBytes) + 8 + 8 + 8 + len(merkleTreeBytes) + 8
	fw.Write(nil, true, IntToBytes(int64(metadataLength)))

}

func IntToBytes(n int64) []byte {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, uint64(n))
	return buf
}

func SizeAndValueToBytes(value string) []byte {
	valueBytes := []byte(value)
	valueSizeBytes := IntToBytes(int64(len(valueBytes)))
	return append(valueSizeBytes, valueBytes...)
}
