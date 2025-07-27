package ss_parser

import (
	"fmt"
	"nosqlEngine/src/models/bloom_filter"
	"nosqlEngine/src/models/key_value"
	"nosqlEngine/src/service/file_writer"
)

type SSParserImpl struct {
	fileWriter file_writer.FileWriterInterface
}

func NewSSParser(fileWriter file_writer.FileWriterInterface) *SSParserImpl {
	return &SSParserImpl{fileWriter: fileWriter}
}

func (ssParser *SSParserImpl) FlushMemtable(data []key_value.KeyValue) {
	key_value.SortByKeys(&data)
	filter := bloom_filter.NewBloomFilter()
	filter.AddMultiple(key_value.GetKeys(data))
	//_ = merkle_tree.GetMerkleTree(data)

	keys, offsets := SerializeDataGetOffsets(ssParser.fileWriter, data)
	ssParser.fileWriter.Write(nil, true, nil) // Write end of section marker

	sumKeys, sumOffsets := SerializeIndexGetOffsets(keys, offsets, ssParser.fileWriter)
	initialSummaryOffset := ssParser.fileWriter.Write(nil, true, nil)

	SerializeSummary(sumKeys, sumOffsets, ssParser.fileWriter)
	bt_bf, _ := filter.SerializeToByteArray()
	prefixFilter := bloom_filter.NewPrefixBloomFilter()
	prefixFilter.AddMultiple(key_value.GetKeys(data))
	bt_pbf, _ := prefixFilter.SerializeToByteArray()
	fmt.Print("PARSER Prefix Bloom Filter: ", bt_pbf, " bytes\n")
	SerializeMetaData(ssParser.fileWriter.Write(nil, true, nil), bt_bf, make([]byte, 0), len(data), ssParser.fileWriter, initialSummaryOffset, bt_pbf)

	// Reset the file writer for the next flush
	ssParser.fileWriter.ResetFileWriter("")
}
