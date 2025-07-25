package ss_compacter

import (
	"nosqlEngine/src/config"
	"nosqlEngine/src/models/bloom_filter"
	"nosqlEngine/src/service/file_writer"
	"nosqlEngine/src/service/ss_parser"
)

var CONFIG = config.GetConfig()

type SSCompacterST struct {
}

func NewSSCompacterST() *SSCompacterST {
	return &SSCompacterST{}
}

func (sc *SSCompacterST) CheckIfCompactionNeeded() bool {
	// to be implemented
	return true
}

func (sc *SSCompacterST) compactTables(tables []string, fw *file_writer.FileWriter) {
	counts := make([]int, len(tables))          // holds the number of items in each table
	readers := make([]*FileReader, len(tables)) // holds the file readers for each table
	keyBytes := make([][]byte, len(tables))     // holds the values read from each table
	totalItems := 0                             // total number of items across all tables
	for i, table := range tables {
		// metadata Nededde
		totalItems = 0
		readers[i] = &FileReader{location: table}
		keyBytes[i] = readers[i].ReadNextVal() // gets next entry (key size, key) || (<value size, value>)
	}
	keys := []string{}
	blockOffsets := []int{}
	currBlockOffset := -1

	bloom := bloom_filter.NewBloomFilter() //
	// merkle := merkle_tree.InitializeMerkleTree(totalItems)
	for !areAllValuesZero(counts) {
		minIndex := getMinValIndex(keyBytes)
		removeDuplicateKeys(keyBytes, minIndex, readers)
		valBytes := readers[minIndex].ReadNextVal() // gets next entry (key size, key) || (<value size, value>)
		if string(valBytes) == CONFIG.Tombstone {
			counts[minIndex]--
			keyBytes[minIndex] = nil // Mark as used
		} else {
			bloom.Add(string(keyBytes[minIndex]))
			// merkle.AddLeaf(string(keyBytes[minIndex]), valBytes) // Add to Merkle tree
			fullVal := append(ss_parser.SizeAndValueToBytes(string(keyBytes[minIndex])), ss_parser.SizeAndValueToBytes(string(valBytes))...)
			newBlockOffset := fw.Write(fullVal, false, nil)
			if currBlockOffset != newBlockOffset {
				currBlockOffset = newBlockOffset
				keys = append(keys, string(keyBytes[minIndex]))
				blockOffsets = append(blockOffsets, currBlockOffset)

			}
			counts[minIndex]-- // Decrease the count for this table
		}
		updateValsAndCounts(keyBytes, counts, readers)
	}
	fw.Write(nil, true, nil) // Write end of file marker

	summaryKeys, summaryOffsets := ss_parser.SerializeIndexGetOffsets(keys, blockOffsets, fw) // Write index offsets
	initialSummaryOffset := fw.Write(nil, true, nil)

	ss_parser.SerializeSummary(summaryKeys, summaryOffsets, fw)

	bt_bf, _ := bloom.SerializeToByteArray()                                                                            // Write summary
	ss_parser.SerializeMetaData(fw.Write(nil, true, nil), bt_bf, make([]byte, 0), totalItems, fw, initialSummaryOffset) // Write metadata
}
