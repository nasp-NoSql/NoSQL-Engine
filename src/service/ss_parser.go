package service

import (
	"encoding/binary"
	"nosqlEngine/src/models/bloom_filter"
	"nosqlEngine/src/models/key_value"
	"nosqlEngine/src/models/merkle_tree"
)

type MemValues struct {
	values []key_value.KeyValue // holds all memtable values that need to be written to SS
}

type SSParser struct {
	mems       []MemValues
	isParsing  bool // flag to check if SS is being written
	fileWriter FileWriter
}

func NewSSParser(fileWriter FileWriter) *SSParser {
	return &SSParser{mems: make([]MemValues, 0), isParsing: false, fileWriter: fileWriter}
}

func (ssParser *SSParser) AddMemtable(keyValues []key_value.KeyValue) {
	memValues := MemValues{values: keyValues}
	ssParser.mems = append(ssParser.mems, memValues)
	ssParser.parseNextMem()
}

func (ssParser *SSParser) parseNextMem() {

	/*
		Checks if SS is being written, if not, then it writes the next memtable to SS to avoid collision

		SSTable format:
		- Data section: contains all values
		- Index section: contains all keys and their offsets in the data section
		- Summary section: contains every 20th key and their offsets in the index section
		- MetaData section: contains the bloom filter, merkle tree, start offset of the summary section and the size of the summary section

		1. Serialize data section - 8 bytes for size of value, value
		2. Serialize index section - 8 bytes for size of key, key, 8 bytes for offset in data section
		3. Serialize summary section - 8 bytes for size of key, key, 8 bytes for offset in index section
		4. Serialize meta data section - bloom filter, merkle tree, /*size of bloom  8 bytes for start offset of summary section, 8 bytes for size of summary section
		5. Write to SS

		Reading from SS:
		1. Read meta data section: last 16 bytes contain the start offset and size of the summary section
		...
	*/
	if ssParser.isParsing {
		return
	}
	ssParser.isParsing = true

	data := ssParser.mems[0].values
	ssParser.mems = ssParser.mems[1:]

	key_value.SortByKeys(&data)

	_ = bloom_filter.GetBloomFilterArray(key_value.GetKeys(data))
	_ = merkle_tree.GetMerkleTree(data)

	dataBytes, dataOffsets := serializeDataGetOffsets(key_value.GetValues(data))

	indexBytes, indexOffsets := serializeIndexGetOffsets(key_value.GetKeys(data), dataOffsets, int64(len(dataBytes)))
	summaryBytes := getSummaryBytes(key_value.GetKeys(data), indexOffsets)
	metaDataBytes := getMetaDataBytes(int64(len(dataBytes)+len(indexBytes)), int64(len(summaryBytes)), make([]byte, 0), make([]byte, 0))

	bytes := make([]byte, 0, len(dataBytes)+len(indexBytes)+len(summaryBytes))
	bytes = append(bytes, dataBytes...)
	bytes = append(bytes, indexBytes...)
	bytes = append(bytes, summaryBytes...)
	bytes = append(bytes, metaDataBytes...)

	ssParser.fileWriter.WriteSS(bytes)

	if len(ssParser.mems) != 0 {
		ssParser.parseNextMem()
	} else {
		ssParser.isParsing = false
	}
}

func serializeDataGetOffsets(values []string) ([]byte, []int64) {
	offsets := make([]int64, 0, len(values))
	dataBytes := make([]byte, 0)

	currOfset := int64(0)
	for i := 0; i < len(values); i++ {
		valueBytes := []byte(values[i])
		valueSizeBytes := intToBytes(int64(len(valueBytes)))
		dataBytes = append(dataBytes, valueSizeBytes...)
		dataBytes = append(dataBytes, valueBytes...)

		offsets = append(offsets, currOfset)
		currOfset += 8 + int64(len(valueBytes))
	}

	return dataBytes, offsets
}

func serializeIndexGetOffsets(keys []string, offsets []int64, startOffset int64) ([]byte, []int64) {
	indexOffsets := make([]int64, 0, len(keys))
	dataBytes := make([]byte, 0)
	currOffset := startOffset

	for i := 0; i < len(keys); i++ {
		keyBytes := []byte(keys[i])
		keySizeBytes := intToBytes(int64(len(keyBytes)))
		dataBytes = append(dataBytes, keySizeBytes...)
		dataBytes = append(dataBytes, keyBytes...)
		dataBytes = append(dataBytes, intToBytes(offsets[i])...)

		indexOffsets = append(indexOffsets, currOffset)
		currOffset += int64(len(keySizeBytes)) + 16
	}
	return dataBytes, indexOffsets
}
func getSummaryBytes(keys []string, offsets []int64) []byte {
	dataBytes := make([]byte, 0)
	for i := 0; i < len(keys); i = i + 20 {
		keyBytes := []byte(keys[i])
		keySizeBytes := intToBytes(int64(len(keyBytes)))
		dataBytes = append(dataBytes, keySizeBytes...)
		dataBytes = append(dataBytes, keyBytes...)
		dataBytes = append(dataBytes, intToBytes(offsets[i])...)
	}
	return dataBytes
}
func getMetaDataBytes(summaryStartOffset int64, summarySize int64, bloomFilterBytes []byte, merkleTreeBytes []byte) []byte {
	dataBytes := make([]byte, 0)
	dataBytes = append(dataBytes, bloomFilterBytes...)
	dataBytes = append(dataBytes, merkleTreeBytes...)
	dataBytes = append(dataBytes, intToBytes(summaryStartOffset)...)
	dataBytes = append(dataBytes, intToBytes(summarySize)...)
	return dataBytes
}

func intToBytes(n int64) []byte {
	buf := make([]byte, 8) // 8 bytes for int64
	binary.BigEndian.PutUint64(buf, uint64(n))
	return buf
}
