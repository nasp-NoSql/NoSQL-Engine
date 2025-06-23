package ss_parser

import (
	"encoding/binary"
	"nosqlEngine/src/models/key_value"
)

const BLOCK_SIZE = 30

func serializeDataGetOffsets(keyValues []key_value.KeyValue) ([]byte, []string, []int64) {

	currOffset := int64(0)
	keys := []string{keyValues[0].GetKey()}
	offsets := []int64{currOffset}
	dataBytes := []byte{}
	currBlockSize := 0

	for i := 0; i < len(keyValues); i++ {
		// key and value blocks
		value := append(sizeAndValueToBytes(keyValues[i].GetKey()), sizeAndValueToBytes(keyValues[i].GetValue())...)

		if len(value)+currBlockSize > BLOCK_SIZE { // if block is full add to dataBytes
			if currBlockSize != 0 { // if value is not bigger then the whole block
				keys = append(keys, keyValues[i].GetKey()) // keyValue| keyValue |keyValue
				offsets = append(offsets, currOffset)
				currBlockSize = len(value)
			}
			dataBytes = append(dataBytes, value...)
			currOffset += int64(len(value))
			continue
		}
		dataBytes = append(dataBytes, value...)
		currOffset += int64(len(value))
		currBlockSize += len(value)
	}
	return dataBytes, keys, offsets
}

func serializeIndexGetOffsets(keys []string, keyOffsets []int64, startOffset int64) ([]byte, []int64) {

	currOffset := startOffset
	indexOffsets := []int64{currOffset}
	dataBytes := make([]byte, 0)
	currBlockSize := 0

	for i := 0; i < len(keys); i++ {
		value := append(sizeAndValueToBytes(keys[i]), intToBytes(keyOffsets[i])...)
		if len(value)+currBlockSize > BLOCK_SIZE { // if block is full add to dataBytes
			if currBlockSize != 0 { // if value is not bigger then the whole block
				indexOffsets = append(indexOffsets, currOffset)
				currBlockSize = len(value)
			}
			dataBytes = append(dataBytes, value...)
			currOffset += int64(len(value))
			continue
		}
		dataBytes = append(dataBytes, value...)
		currOffset += int64(len(value))
		currBlockSize += len(value)
	}
	return dataBytes, indexOffsets
}
func getSummaryBytes(keys []string, offsets []int64) []byte {

	dataBytes := make([]byte, 0)
	RANGE_SIZE := 20

	for i := 0; i < len(keys); i = i + RANGE_SIZE {
		value := append(sizeAndValueToBytes(keys[i]), intToBytes(offsets[i])...)
		dataBytes = append(dataBytes, value...)
	}
	return dataBytes
}
func getMetaDataBytes(summarySize int64, summaryStartOffset int64, bloomFilterBytes []byte, merkleTreeBytes []byte) []byte {
	dataBytes := make([]byte, 0)
	dataBytes = append(dataBytes, intToBytes(int64(len(bloomFilterBytes)))...)
	dataBytes = append(dataBytes, bloomFilterBytes...)
	dataBytes = append(dataBytes, intToBytes(int64(len(merkleTreeBytes)))...)
	dataBytes = append(dataBytes, merkleTreeBytes...)
	dataBytes = append(dataBytes, intToBytes(summarySize)...)
	dataBytes = append(dataBytes, intToBytes(summaryStartOffset)...)
	dataBytes = append(dataBytes, intToBytes(int64(len(dataBytes)))...)
	return dataBytes
}
func intToBytes(n int64) []byte {
	buf := make([]byte, 8) // 8 bytes for int64
	binary.BigEndian.PutUint64(buf, uint64(n))
	return buf
}

func sizeAndValueToBytes(value string) []byte {
	valueBytes := []byte(value)
	valueSizeBytes := intToBytes(int64(len(valueBytes)))
	return append(valueSizeBytes, valueBytes...)
}
