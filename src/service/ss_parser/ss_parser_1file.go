package ss_parser

import (
	"nosqlEngine/src/models/bloom_filter"
	"nosqlEngine/src/models/key_value"
	"nosqlEngine/src/service/file_writer"
)

type MemValues struct {
	values []key_value.KeyValue // holds all memtable values that need to be written to SS
}

type SSParser1File struct {
	mems       []MemValues
	isParsing  bool // flag to check if SS is being written
	fileWriter file_writer.FileWriter
}

func NewSSParser1File(fileWriter file_writer.FileWriter) SSParser {
	return &SSParser1File{mems: make([]MemValues, 0), isParsing: false, fileWriter: fileWriter}
}

func (ssParser *SSParser1File) AddMemtable(keyValues []key_value.KeyValue) {
	memValues := MemValues{values: keyValues}
	ssParser.mems = append(ssParser.mems, memValues)
	ssParser.parseNextMem()
}

func (ssParser *SSParser1File) parseNextMem() {
	if ssParser.isParsing {
		return
	}
	ssParser.isParsing = true

	data := ssParser.mems[0].values
	ssParser.mems = ssParser.mems[1:]

	key_value.SortByKeys(&data)

	bloom := bloom_filter.GetBloomFilterArray(key_value.GetKeys(data))
	//_ = merkle_tree.GetMerkleTree(data)

	dataBytes, keys, keyOffsets := serializeDataGetOffsets(data)
	indexBytes, indexOffsets := serializeIndexGetOffsets(keys, keyOffsets, int64(len(dataBytes)))
	summaryBytes := getSummaryBytes(key_value.GetKeys(data), indexOffsets)
	summaryOffset := int64(len(dataBytes) + len(indexBytes))
	// currently holder 0 bytes for merkle tree
	metaDataBytes := getMetaDataBytes(int64(len(summaryBytes)), summaryOffset, bloom, make([]byte, 0), int64(len(data)))
	currentFileSize := len(dataBytes) + len(indexBytes) + len(summaryBytes) // without metadata
	metaDataBytes = addPaddingToBlock(metaDataBytes, currentFileSize  + len(metaDataBytes), CONFIG.BlockSize, false) // padding metadata so the file is a  multiple of BlockSize
	currentFileSize += len(metaDataBytes)
	
	bytes := make([]byte, 0, currentFileSize)
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
