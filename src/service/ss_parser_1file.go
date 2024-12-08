package service

import (
	"encoding/binary"
	"fmt"
	"nosqlEngine/src/models/bloom_filter"
	"nosqlEngine/src/models/key_value"
)

const BLOCK_SIZE = 30

type MemValues struct {
	values []key_value.KeyValue // holds all memtable values that need to be written to SS
}

type SSParser1File struct {
	mems       []MemValues
	isParsing  bool // flag to check if SS is being written
	fileWriter FileWriter
}

func NewSSParser1File(fileWriter FileWriter) SSParser {
	return &SSParser1File{mems: make([]MemValues, 0), isParsing: false, fileWriter: fileWriter}
}

func (ssParser *SSParser1File) AddMemtable(keyValues []key_value.KeyValue) {
	memValues := MemValues{values: keyValues}
	ssParser.mems = append(ssParser.mems, memValues)
	ssParser.parseNextMem()
}

func (ssParser *SSParser1File) parseNextMem() {

	/*
		Checks if SS is being written, if not, then it writes the next memtable to SS to avoid collision

		SSTable format:
		1. Data section:8 bytes for key size, key, 8 bytes for size of value, value
		2. Index section: 8 bytes for size of key, key, 8 bytes for offset in data section
		3. Summary section: 8 bytes for size of key, key, 8 bytes for offset in index section
		4. MetaData section: 8 bytes summary size, 8 bytes summary start offset,  8 bytes merkle tree size, merkle tree 8 bytes bloom filter size, bloom filter, 8 byters filter size

	*/
	if ssParser.isParsing {
		return
	}
	ssParser.isParsing = true

	data := ssParser.mems[0].values
	ssParser.mems = ssParser.mems[1:]

	key_value.SortByKeys(&data)

	_ = bloom_filter.GetBloomFilterArray(key_value.GetKeys(data))
	//_ = merkle_tree.GetMerkleTree(data)

	dataBytes, keys, keyOffsets := serializeDataGetOffsets(data)
	indexBytes, indexOffsets := serializeIndexGetOffsets(keys, keyOffsets, int64(len(dataBytes)))
	summaryBytes := getSummaryBytes(key_value.GetKeys(data), indexOffsets)
	summaryOffset := int64(len(dataBytes) + len(indexBytes))
	metaDataBytes := getMetaDataBytes(int64(len(summaryBytes)), summaryOffset, make([]byte, 0), make([]byte, 0), int64(len(data)))

	bytes := make([]byte, 0, len(dataBytes)+len(indexBytes)+len(summaryBytes)+len(metaDataBytes))
	bytes = append(bytes, dataBytes...)
	bytes = append(bytes, indexBytes...)
	bytes = append(bytes, summaryBytes...)
	bytes = append(bytes, metaDataBytes...)
	fmt.Print("Data length: ", len(bytes), " in  parser \n")
	if ssParser.fileWriter.WriteSS(bytes) {
		fmt.Print("SS written successfully")
	} else {

		fmt.Print("SS write failed")
	}

	if len(ssParser.mems) != 0 {
		ssParser.parseNextMem()
	} else {
		ssParser.isParsing = false
	}
}

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
func getMetaDataBytes(summarySize int64, summaryStartOffset int64, bloomFilterBytes []byte, merkleTreeBytes []byte, dataSize int64) []byte {
	dataBytes := make([]byte, 0)
	dataBytes = append(dataBytes, intToBytes(int64(len(bloomFilterBytes)))...)
	dataBytes = append(dataBytes, bloomFilterBytes...)
	dataBytes = append(dataBytes, intToBytes(int64(len(merkleTreeBytes)))...)
	dataBytes = append(dataBytes, merkleTreeBytes...)
	dataBytes = append(dataBytes, intToBytes(summarySize)...)
	dataBytes = append(dataBytes, intToBytes(summaryStartOffset)...)
	dataBytes = append(dataBytes, intToBytes(dataSize)...)
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
