package service

import (
	"encoding/binary"
	"nosqlEngine/src/models/bloom_filter"
	"nosqlEngine/src/models/key_value"
	"nosqlEngine/src/models/merkle_tree"
)
const BLOCK_SIZE = 4096
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
		1. Data section: 8 bytes fo padding size at the end of the block, 8 bytes for size of value, value
		2. Index section: 8 bytes for size of key, key, 8 bytes for offset in data section
		3. Summary section: 8 bytes for size of key, key, 8 bytes for offset in index section
		4. MetaData section: 8 bytes for size of summary section, merkle tree, 8 bytes for merkle tree size, bloom filter, 8 bytes for bloom size
		
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

	dataBytes, keys, keyOffsets := serializeDataGetOffsets(key_value.GetValues(data))
	indexBytes, indexOffsets := serializeIndexGetOffsets(keys, keyOffsets, len(dataBytes))
	summaryBytes := getSummaryBytes(key_value.GetKeys(data), indexOffsets)
	metaDataBytes := getMetaDataBytes(int64(len(summaryBytes)), make([]byte, 0), make([]byte, 0))

	bytes := make([]byte, 0, len(dataBytes)+len(indexBytes)+len(summaryBytes)+len(metaDataBytes))
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

func serializeDataGetOffsets(keyValues []key_value.KeyValue) ([]byte, []string, []int64) {

	keys := make([]string, 0)
	offsets := make([]int64, 0)
	currOffset := int64(0)
	keys = append(keys, keyValues[0].key)
	offsets = append(offsets, currOffset)

	dataBytes := make([]byte, 0)
	blockBytes := make([]byte, BLOCK_SIZE)

	for i := 0; i < len(keyValues); i++ {

		valueBytes := []byte(keyValues[i].GetValue())
		valueSizeBytes := intToBytes(int64(len(valueBytes)))
		valueBlockBytes := append(valueSizeBytes, valueBytes...)
		
		if len(blockBytes) + 8 + len(valueBlockBytes) > BLOCK_SIZE { // if block is full, add padding
			paddingSize := BLOCK_SIZE - len(blockBytes) - 8
			paddingSizeBytes := intToBytes(int64(paddingSize))

			blockBytes = append(blockBytes, paddingSizeBytes...)
			blockBytes = append(blockBytes,  make([]byte, paddingSize)...)
			
			dataBytes = append(dataBytes, blockBytes...)
			blockBytes = make([]byte, 0)
		
			offsets = append(offsets, currOffset)
			currOffset += BLOCK_SIZE
			if i+1 < len(keyValues) {
				keys = append(keys, keyValues[i+1].key)
				offsets = append(offsets, currOffset)
			}
			continue
		}
		blockBytes = append(blockBytes, valueSizeBytes...)
		blockBytes = append(blockBytes, valueBytes...)
	}
	if len(blockBytes) == 0 {
		return dataBytes, keys, offsets
	}
	// last padding 
	paddingSize := BLOCK_SIZE - len(blockBytes) - 8
	paddingSizeBytes := intToBytes(int64(paddingSize))
	blockBytes = append(blockBytes, paddingSizeBytes...)
	blockBytes = append(blockBytes,  make([]byte, paddingSize)...)
	dataBytes = append(dataBytes, blockBytes...)

	return dataBytes, keys, offsets
}
func serializeIndexGetOffsets(keys []string, keyOffsets []int64, startOffset int64) ([]byte, []int64) {
	indexOffsets := make([]int64, 0, len(keys))
	dataBytes := make([]byte, 0)
	currOffset := startOffset

	for i := 0; i < len(keys); i++ {
		keyBytes := []byte(keys[i])
		keySizeBytes := intToBytes(int64(len(keyBytes)))
		dataBytes = append(dataBytes, keySizeBytes...)
		dataBytes = append(dataBytes, keyBytes...)
		dataBytes = append(dataBytes, intToBytes(keyOffsets[i])...)

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
func getMetaDataBytes(summarySize int64, bloomFilterBytes []byte, merkleTreeBytes []byte) []byte {
	dataBytes := make([]byte, 0)
	dataBytes = append(dataBytes, intToBytes(summarySize)...)
	dataBytes = append(dataBytes, merkleTreeBytes...)
	dataBytes = append(dataBytes, intToBytes(int64(len(merkleTreeBytes)))...)
	dataBytes = append(dataBytes, bloomFilterBytes...)
	dataBytes = append(dataBytes, intToBytes(int64(len(bloomFilterBytes)))...)
	
	return dataBytes
}
func intToBytes(n int64) []byte {
	buf := make([]byte, 8) // 8 bytes for int64
	binary.BigEndian.PutUint64(buf, uint64(n))
	return buf
}
