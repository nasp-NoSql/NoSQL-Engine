package ss_parser

import (
	"encoding/binary"
	"nosqlEngine/src/config"
	"nosqlEngine/src/models/key_value"
)

var CONFIG = config.GetConfig()


func serializeDataGetOffsets(keyValues []key_value.KeyValue) ([]byte, []string, []int64) {

	currOffset := int64(0)
	keys := []string{keyValues[0].GetKey()}
	offsets := []int64{currOffset}
	dataBytes := []byte{}
	currBlockSize := 0

	for i := 0; i < len(keyValues); i++ {
		// key and value blocks
		value := append(sizeAndValueToBytes(keyValues[i].GetKey()), sizeAndValueToBytes(keyValues[i].GetValue())...)

		if currBlockSize >= CONFIG.BlockSize { // if last value got over block size, this key starts a new block
			keys = append(keys, keyValues[i].GetKey())
			offsets = append(offsets, currOffset)
			currBlockSize = currBlockSize % CONFIG.BlockSize + len(value)
		} else{
			currBlockSize += len(value)
		}
		dataBytes = append(dataBytes, value...)
		currOffset += int64(len(value))
	}
	return dataBytes, keys, offsets
}

func serializeIndexGetOffsets(keys []string, keyOffsets []int64, startOffset int64) ([]byte, []int64) {

	currOffset := startOffset
	indexOffsets := []int64{}
	dataBytes := make([]byte, 0)

	for i := 0; i < len(keys); i++ {
		value := append(sizeAndValueToBytes(keys[i]), intToBytes(keyOffsets[i])...)
		dataBytes = append(dataBytes, value...)
		indexOffsets = append(indexOffsets, currOffset)
		currOffset += int64(len(value))
	}
	return dataBytes, indexOffsets
}
func getSummaryBytes(keys []string, offsets []int64) []byte {

	dataBytes := make([]byte, 0)

	for i := 0; i < len(keys); i = i + CONFIG.SummaryStep {
		value := append(sizeAndValueToBytes(keys[i]), intToBytes(offsets[i])...)
		dataBytes = append(dataBytes, value...)
	}
	return dataBytes
}
func getMetaDataBytes(summarySize int64, summaryStartOffset int64, bloomFilterBytes []byte, merkleTreeBytes []byte, numOfItems int64) []byte {
	dataBytes := make([]byte, 0)
	dataBytes = append(dataBytes, merkleTreeBytes...)
	dataBytes = append(dataBytes, intToBytes(int64(len(merkleTreeBytes)))...)
	dataBytes = append(dataBytes, intToBytes(numOfItems)...)
	dataBytes = append(dataBytes, intToBytes(summarySize)...)
	dataBytes = append(dataBytes, intToBytes(summaryStartOffset)...)
	dataBytes = append(dataBytes, bloomFilterBytes...)
	dataBytes = append(dataBytes, intToBytes(int64(len(bloomFilterBytes)))...)
	return dataBytes
}
func intToBytes(n int64) []byte {
	buf := make([]byte, 8) // 8 bytes for int64
	binary.BigEndian.PutUint64(buf, uint64(n))
	return buf
}
func addPaddingToBlock(data []byte,dataSize int, size int, fromBack bool) []byte {
	if dataSize % size != 0 {
		paddingSize := size - (dataSize % size)
		padding := make([]byte, paddingSize)
		if fromBack {
		data = append(data, padding...)
		} else {
			data = append(padding, data...)
		}
	}
	return data
}
func sizeAndValueToBytes(value string) []byte {
	valueBytes := []byte(value)
	valueSizeBytes := intToBytes(int64(len(valueBytes)))
	return append(valueSizeBytes, valueBytes...)
}
