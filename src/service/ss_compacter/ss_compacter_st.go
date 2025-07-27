package ss_compacter

import (
	"fmt"
	"nosqlEngine/src/config"
	"nosqlEngine/src/models/bloom_filter"
	"nosqlEngine/src/service/block_manager"
	"nosqlEngine/src/service/file_writer"
	"nosqlEngine/src/service/retriever"
	"nosqlEngine/src/service/ss_parser"
	"os"
	"path/filepath"
	"strings"

	"github.com/google/uuid"
)

var CONFIG = config.GetConfig()

type SSCompacterST struct {
}

func NewSSCompacterST() *SSCompacterST {
	return &SSCompacterST{}
}

func (sc *SSCompacterST) CheckCompactionConditions(bm *block_manager.BlockManager) bool {
	baseDir := "../../../"+CONFIG.LSMBaseDir
	level := 0
	compacted := false
	for level < CONFIG.LSMLevels {
		lvlDir := filepath.Join(baseDir, fmt.Sprintf("lvl%d", level))
		files, err := os.ReadDir(lvlDir)
		if err != nil {
			// If the directory doesn't exist, stop
			break
		}
		var sstFiles []string

		for _, f := range files {
			if !f.IsDir() && (strings.HasSuffix(f.Name(), ".db") || strings.HasSuffix(f.Name(), ".sst")) {
				sstFiles = append(sstFiles, filepath.Join(lvlDir, f.Name()))
			}
		}
		for len(sstFiles) >= CONFIG.CompactionThreshold && level < CONFIG.LSMLevels {
			toCompact := sstFiles[:CONFIG.CompactionThreshold]
			sstFiles = sstFiles[CONFIG.CompactionThreshold:]
			// You may need to create a FileWriter for the next level here
			lvlDir := fmt.Sprintf("lvl%d", level+1)
			fw := file_writer.NewFileWriter(bm, CONFIG.BlockSize, "sstable/"+lvlDir+"/sstable_"+uuid.New().String()+".db")
			sc.compactTables(toCompact, fw, bm)
			compacted = true
		}
		level++
	}
	return compacted
}

func (sc *SSCompacterST) compactTables(tables []string, fw *file_writer.FileWriter, bm *block_manager.BlockManager) {
	counts := make([]int, len(tables)) // holds the number of items in each table
	currKeys := make([]string, len(tables))
	currValues := make([]string, len(tables))
	pool := retriever.NewEntryRetrieverPool(bm, tables)
	totalItems := 0                                    // total number of items across all tables
	for i := range tables {
		counts[i] = int(pool.GetMetadata(i).Getnum_of_items())
		em:= fmt.Errorf("Error getting metadata for table")
		totalItems += counts[i]
		currKeys[i], currValues[i], _, em = pool.ReadNextVal(i) // Read the first key and value from each table
		fmt.Print(em)
	}
	fmt.Print(currKeys)

	keys := []string{}
	blockOffsets := []int{}
	currBlockOffset := -1

	bloom := bloom_filter.NewBloomFilterWithParams(totalItems, 0.01) // 1% false positive rate
	// merkle := merkle_tree.InitializeMerkleTree(totalItems)
	for !areAllValuesZero(counts) {
		minIndex := getMinValIndex(currKeys)
		removeDuplicateKeys(currKeys, minIndex, pool)
		bloom.Add(currKeys[minIndex])
		// merkle.AddLeaf(string(keyBytes[minIndex]), valBytes) // Add to Merkle tree
		fullVal := append(ss_parser.SizeAndValueToBytes(currKeys[minIndex]), ss_parser.SizeAndValueToBytes(currValues[minIndex])...)
		newBlockOffset := fw.Write(fullVal, false, nil)
		if currBlockOffset != newBlockOffset {
			currBlockOffset = newBlockOffset
			keys = append(keys, currKeys[minIndex])
			blockOffsets = append(blockOffsets, currBlockOffset)

		}
		currKeys[minIndex] = "" 
		updateValsAndCounts(currKeys, currValues, counts, pool)
	}

	fw.Write(nil, true, nil) // Write end of file marker
	summaryKeys, summaryOffsets := ss_parser.SerializeIndexGetOffsets(keys, blockOffsets, fw) // Write index offsets
	initialSummaryOffset := fw.Write(nil, true, nil)

	ss_parser.SerializeSummary(summaryKeys, summaryOffsets, fw)
	prefixFilter := bloom_filter.NewPrefixBloomFilter()

	bt_pbf, _ := prefixFilter.SerializeToByteArray()
	bt_bf, _ := bloom.SerializeToByteArray()          
	ss_parser.SerializeMetaData(fw.Write(nil, true, nil), bt_bf, make([]byte, 0), totalItems, fw, initialSummaryOffset, bt_pbf) // Write metadata
}
