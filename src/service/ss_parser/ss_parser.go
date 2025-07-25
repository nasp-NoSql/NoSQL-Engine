package ss_parser

import (
	"fmt"
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

	filter := bloom_filter.NewBloomFilter()
	filter.AddMultiple(key_value.GetKeys(data))

	fmt.Printf("Bf bytearray: %v\n", filter.GetArray())

	//_ = merkle_tree.GetMerkleTree(data)

	keys, offsets := SerializeDataGetOffsets(ssParser.fileWriter, data)
	ssParser.fileWriter.Write(nil, true, nil) // Write end of section marker

	sumKeys, sumOffsets := SerializeIndexGetOffsets(keys, offsets, ssParser.fileWriter)
	initialSummaryOffset := ssParser.fileWriter.Write(nil, true, nil)

	SerializeSummary(sumKeys, sumOffsets, ssParser.fileWriter)
	bt_bf, _ := filter.SerializeToByteArray()
	SerializeMetaData(ssParser.fileWriter.Write(nil, true, nil), bt_bf, make([]byte, 0), len(data), ssParser.fileWriter, initialSummaryOffset)
	if len(ssParser.mems) != 0 {
		ssParser.parseNextMem()
	} else {
		ssParser.isParsing = false
	}
}
