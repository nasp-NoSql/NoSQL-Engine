package ss_parser

import (
	"nosqlEngine/src/models/bloom_filter"
	"nosqlEngine/src/models/key_value"
	"nosqlEngine/src/service/file_writer"
)

type SSParserMfile struct {
	mems       []MemValues
	isParsing  bool // flag to check if SS is being written
	fileWriter file_writer.FileWriter
}

func NewSSParserMfile(fileWriter file_writer.FileWriter) SSParser {
	return &SSParserMfile{mems: make([]MemValues, 0), isParsing: false, fileWriter: fileWriter}
}

func (ssParser *SSParserMfile) AddMemtable(keyValues []key_value.KeyValue) {
	memValues := MemValues{values: keyValues}
	ssParser.mems = append(ssParser.mems, memValues)
	ssParser.parseNextMem()
}

func (ssParser *SSParserMfile) parseNextMem() {

	/*
		Checks if SS is being written, if not, then it writes the next memtable to SS to avoid collision

		SSTable format:
		1. Data section:8 bytes for key size, key, 8 bytes for size of value, value
		2. Index section: 8 bytes for size of key, key, 8 bytes for offset in data section
		3. Summary section: 8 bytes for size of key, key, 8 bytes for offset in index section

	*/
	if ssParser.isParsing {
		return
	}
	ssParser.isParsing = true

	data := ssParser.mems[0].values
	ssParser.mems = ssParser.mems[1:]

	key_value.SortByKeys(&data)

	_ = bloom_filter.GetBloomFilterArray(key_value.GetKeys(data))
	//	_ = merkle_tree.GetMerkleTree(data)

	dataBytes, keys, keyOffsets := serializeDataGetOffsets(data)
	indexBytes, indexOffsets := serializeIndexGetOffsets(keys, keyOffsets, int64(0))
	summaryBytes := getSummaryBytes(key_value.GetKeys(data), indexOffsets)
	metaDataBytes := getMetaDataBytes(int64(len(indexBytes)), int64(len(summaryBytes)), int64(0), make([]byte, 0), make([]byte, 0), int64(len(data)))

	ssParser.fileWriter.WriteSS(dataBytes, indexBytes, summaryBytes, metaDataBytes)

	if len(ssParser.mems) != 0 {
		ssParser.parseNextMem()
	} else {
		ssParser.isParsing = false
	}
}
