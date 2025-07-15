package ss_parser

import (
	"encoding/binary"
	"nosqlEngine/src/config"
	"nosqlEngine/src/models/key_value"
	"nosqlEngine/src/service/file_writer"
)

var CONFIG = config.GetConfig()

func SerializeDataGetOffsets(fw *file_writer.FileWriter, keyValues []key_value.KeyValue) ([]string, []int) {
	keys := []string{keyValues[0].GetKey()}
	offsets := []int{}
	currBlockIndex := -1
	for i := 0; i < len(keyValues); i++ {
		value := append(SizeAndValueToBytes(keyValues[i].GetKey()), SizeAndValueToBytes(keyValues[i].GetValue())...)
		blockIndex := fw.Write(value, false)
		if currBlockIndex != blockIndex {
			currBlockIndex = blockIndex
			keys = append(keys, keyValues[i].GetKey())
			offsets = append(offsets, currBlockIndex)
		}
	}
	return keys, offsets
}

func SerializeIndexGetOffsets(keys []string, keyOffsets []int, fw *file_writer.FileWriter) []int {
	blockIndex := []int{}
	for i := 0; i < len(keys); i++ {
		value := append(SizeAndValueToBytes(keys[i]), IntToBytes(int64(keyOffsets[i]))...)
		currBlock := fw.Write(value, false)
		blockIndex = append(blockIndex, currBlock)
	}
	return blockIndex
}
func SerializeSummary(keys []string, offsets []int, fw *file_writer.FileWriter) {

	for i := 0; i < len(keys); i = i + CONFIG.SummaryStep {
		value := append(SizeAndValueToBytes(keys[i]), IntToBytes(int64(offsets[i]))...)
		fw.Write(value, false)
	}
}
func SerializeMetaData(summaryStartOffset int, bloomFilterBytes []byte, merkleTreeBytes []byte, numOfItems int, fw *file_writer.FileWriter) {
	fw.Write(IntToBytes(int64(len(bloomFilterBytes))), false)
	fw.Write(bloomFilterBytes, false)
	fw.Write(IntToBytes(int64(summaryStartOffset)), false)
	fw.Write(IntToBytes((int64(numOfItems))), false)
	fw.Write(merkleTreeBytes, true)
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
