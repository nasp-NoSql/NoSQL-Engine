package ss_parser

import (
	"nosqlEngine/src/models/bloom_filter"
	"nosqlEngine/src/models/key_value"
	"nosqlEngine/src/service/file_writer"
)

type MemValues struct {
	values []key_value.KeyValue // holds all memtable values that need to be written to SS
}

type SSParserImpl struct {
	mems       []MemValues
	isParsing  bool // flag to check if SS is being written
	fileWriter file_writer.FileWriterInterface
}

func NewSSParser(fileWriter file_writer.FileWriterInterface) *SSParserImpl {
	return &SSParserImpl{mems: make([]MemValues, 0), isParsing: false, fileWriter: fileWriter}
}

func (ssParser *SSParserImpl) AddMemtable(keyValues []key_value.KeyValue) {
	memValues := MemValues{values: keyValues}
	ssParser.mems = append(ssParser.mems, memValues)
	ssParser.parseNextMem()
}

func (ssParser *SSParserImpl) parseNextMem() {
	if ssParser.isParsing {
		return
	}
	ssParser.isParsing = true

	data := ssParser.mems[0].values
	ssParser.mems = ssParser.mems[1:]

	key_value.SortByKeys(&data)

	//bloom, _ := bloom_filter.GetBloomFilterArray(key_value.GetKeys(data))
	filter := bloom_filter.NewBloomFilter()
	filter.AddMultiple(key_value.GetKeys(data))
	//_ = merkle_tree.GetMerkleTree(data)

	keys, keyOffsets := SerializeDataGetOffsets(ssParser.fileWriter, data)
	ssParser.fileWriter.Write(nil, true, nil) // Write end of section marker

	indexOffsets := SerializeIndexGetOffsets(keys, keyOffsets, ssParser.fileWriter)
	ssParser.fileWriter.Write(nil, true, nil)

	SerializeSummary(keys, indexOffsets, ssParser.fileWriter)
	ssParser.fileWriter.Write(nil, true, nil)

	SerializeMetaData(ssParser.fileWriter.Write(nil, true, nil), filter.GetArray(), make([]byte, 0), len(data), ssParser.fileWriter)
	if len(ssParser.mems) != 0 {
		ssParser.parseNextMem()
	} else {
		ssParser.isParsing = false
	}
}
