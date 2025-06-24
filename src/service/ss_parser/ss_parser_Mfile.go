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
	if ssParser.isParsing {
		return
	}
	ssParser.isParsing = true

	data := ssParser.mems[0].values
	ssParser.mems = ssParser.mems[1:]

	key_value.SortByKeys(&data)

	bloom := bloom_filter.GetBloomFilterArray(key_value.GetKeys(data))
//	_ = merkle_tree.GetMerkleTree(data)

	dataBytes, keys, keyOffsets := serializeDataGetOffsets(data)
	indexBytes, indexOffsets := serializeIndexGetOffsets(keys, keyOffsets, int64(0))
	summaryBytes := getSummaryBytes(key_value.GetKeys(data), indexOffsets)
	metaDataBytes := getMetaDataBytes(int64(len(summaryBytes)),int64(0), bloom, make([]byte, 0), int64(len(data)))
	
	// Add padding to every section to be a multiple of BLOCK_SIZE
	dataBytes = addPaddingToBlock(dataBytes, len(dataBytes), CONFIG.BlockSize, true)
	indexBytes = addPaddingToBlock(indexBytes,len(indexBytes), CONFIG.BlockSize, true)
	summaryBytes = addPaddingToBlock(summaryBytes,len(summaryBytes), CONFIG.BlockSize, true)
	metaDataBytes = addPaddingToBlock(metaDataBytes, len(metaDataBytes), CONFIG.BlockSize, false) 
	ssParser.fileWriter.WriteSS(dataBytes, indexBytes, summaryBytes, metaDataBytes)


	if len(ssParser.mems) != 0 {
		ssParser.parseNextMem()
	} else {
		ssParser.isParsing = false
	}
}
