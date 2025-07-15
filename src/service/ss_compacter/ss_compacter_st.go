package sscompacter

import (
	"nosqlEngine/src/config"
	"nosqlEngine/src/service/block_manager"
	"nosqlEngine/src/service/file_writer"
	"nosqlEngine/src/service/ss_parser"
)
var CONFIG = config.GetConfig()

type SSCompacterST struct {
	bm *block_manager.BlockManager
}
type FileReader struct {
	location string
} 
func (fw *FileReader) ReadNextVal() ([]byte) {
	return []byte{0}// Mock implementation, replace with actual reading logic
} 
func (fw *FileReader) Read(size int) ([]byte){
	return make([]byte, size) 
}
func (fw *FileReader) ReadMetaData() (int, []byte, []byte, int) {
	return 0, make([]byte, 0), make([]byte, 0), 0 // Mock implementation, replace with actual reading logic
}


func (sc *SSCompacterST) compactTables(tables []string, fw *file_writer.FileWriter) {
	counts := make([]int, len(tables)) // holds the number of items in each table
	readers := make([]*FileReader, len(tables)) // holds the file readers for each table
	keyBytes := make([][]byte, len(tables)) // holds the values read from each table
	totalItems := 0 // total number of items across all tables
	for i, table := range tables {
		// metadata Nededde
		totalItems = 0
		readers[i] = &FileReader{location: table} 
		keyBytes[i] = readers[i].ReadNextVal()
	}
	keys :=[]string{}
	blockOffsets := []int{}
	currBlockOffset := -1
	
	for !areAllValuesZero(counts) {
		minIndex := getMinValIndex(keyBytes)
		removeDuplicateKeys(keyBytes, minIndex, readers)
		valBytes := readers[minIndex].ReadNextVal()
		if string(valBytes) == CONFIG.Tombstone {
			counts[minIndex]-- 
			keyBytes[minIndex] = nil // Mark as used
		} else {
			fullVal := append(ss_parser.SizeAndValueToBytes(string(keyBytes[minIndex])), ss_parser.SizeAndValueToBytes(string(valBytes))...)
			newBlockOffset := fw.Write(fullVal, false)
			if currBlockOffset != newBlockOffset {
				currBlockOffset = newBlockOffset
				keys = append(keys, string(keyBytes[minIndex]))
				blockOffsets = append(blockOffsets, currBlockOffset)
			
			}
			counts[minIndex]-- // Decrease the count for this table
		}
		updateValsAndCounts(keyBytes, counts, readers)
	}
	fw.Write(nil, true) // Write end of file marker

	indexOffsets := ss_parser.SerializeIndexGetOffsets(keys, blockOffsets, fw) // Write index offsets
	ss_parser.SerializeSummary(keys, indexOffsets, fw) // Write summary
	ss_parser.SerializeMetaData(fw.Write(nil, true), make([]byte, 0), make([]byte, 0), totalItems, fw) // Write metadata
}
