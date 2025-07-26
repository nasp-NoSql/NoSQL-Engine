package retriever

import (
	"fmt"
	"nosqlEngine/src/models/bloom_filter"
	"nosqlEngine/src/service/block_manager"
	"nosqlEngine/src/service/file_reader"
)

type MultiRetriever struct {
	fileReader   file_reader.FileReader
	sstablePaths []string
	currentIndex int
}

func NewMultiRetriever(bm *block_manager.BlockManager) *MultiRetriever {
	sstablePaths := initializeSSTablePool()

	// Create a single block manager and file reader instance
	var fileReader file_reader.FileReader

	if len(sstablePaths) > 0 {
		fileReader = *file_reader.NewFileReader(sstablePaths[0], CONFIG.BlockSize, *bm)
	} else {
		fileReader = *file_reader.NewFileReader("", CONFIG.BlockSize, *bm)
	}

	return &MultiRetriever{
		fileReader:   fileReader,
		sstablePaths: sstablePaths,
		currentIndex: 0,
	}
}
func (mr *MultiRetriever) resetToNextSSTable() bool {
	mr.currentIndex++
	if mr.currentIndex >= len(mr.sstablePaths) {
		return false // No more SSTables to check
	}

	mr.fileReader.ResetReader(mr.sstablePaths[mr.currentIndex], false)
	fmt.Printf("Switched to SSTable: %s\n", mr.sstablePaths[mr.currentIndex])
	return true
}
func (mr *MultiRetriever) deserializeMetadata(key string) (Metadata, error) {

	i := 0
	initial, readBlocks, err := mr.fileReader.ReadEntry(i)
	if err != nil {
		return Metadata{}, fmt.Errorf("error reading block %d: %v, READ %d blocks", i, err, readBlocks)
	}
	i += 1

	mdOffset := bytesToInt(initial[len(initial)-8:])

	totalBlocks, err := mr.fileReader.GetFileSizeBlocks()
	if err != nil {
		return Metadata{}, fmt.Errorf("error getting file size blocks: %v", err)
	}
	numOfBlocks := int64(totalBlocks) - mdOffset
	completedBlocks := make([]byte, 0, int(numOfBlocks)*CONFIG.BlockSize)
	completedBlocks = append(completedBlocks, initial...)
	for i < int(numOfBlocks) {
		block, readBlocks, err := mr.fileReader.ReadEntry(i)
		if err != nil {
			return Metadata{}, fmt.Errorf("error reading block %d: %v", i, err)
		}
		completedBlocks = append(block, completedBlocks...)
		if readBlocks == int(numOfBlocks) {
			break
		}
		i += int(readBlocks)
	}
	offsetInBlock := int64(0)
	completedBlocks = append(completedBlocks, initial...)
	bf_size := bytesToInt(completedBlocks[:8])
	offsetInBlock += 8
	bf_data := completedBlocks[offsetInBlock : offsetInBlock+bf_size]
	offsetInBlock += bf_size

	bf_pb_size := bytesToInt(completedBlocks[offsetInBlock : offsetInBlock+8])
	offsetInBlock += 8
	bf_bp_bytes := completedBlocks[offsetInBlock : offsetInBlock+bf_pb_size]
	// deser prefix bf
	prefixBF, errPbf := bloom_filter.DeserializePrefixBloomFilter(bf_bp_bytes)
	if errPbf != nil {
		return Metadata{}, fmt.Errorf("error deserializing prefix bloom filter")
	}
	ex := prefixBF.Contains(key)
	if !ex {
		return Metadata{}, fmt.Errorf("key %s not found in prefix bloom filter", key)
	}
	offsetInBlock += bf_pb_size
	if err != nil {
		return Metadata{}, fmt.Errorf("error deserializing bloom filter: %v", err)
	}

	sum_start_offset := bytesToInt(completedBlocks[offsetInBlock : offsetInBlock+8])
	offsetInBlock += 8

	sum_end_offset := bytesToInt(completedBlocks[offsetInBlock : offsetInBlock+8])
	offsetInBlock += 8
	blocksInFile, err := mr.fileReader.GetFileSizeBlocks()
	if err != nil {
		return Metadata{}, fmt.Errorf("error getting file size blocks: %v", err)
	}
	sum_start_offset = int64(blocksInFile) - sum_start_offset
	sum_end_offset = int64(blocksInFile) - sum_end_offset

	numOfItems := bytesToInt(completedBlocks[offsetInBlock : offsetInBlock+8])
	offsetInBlock += 8

	merkle_size := bytesToInt(completedBlocks[offsetInBlock : offsetInBlock+8])
	offsetInBlock += 8
	merkle_data := completedBlocks[offsetInBlock : offsetInBlock+merkle_size]

	md := Metadata{
		bf_size:       bf_size,
		bf_data:       bf_data,
		summary_start: sum_start_offset,
		summary_end:   sum_end_offset,
		numOfItems:    numOfItems,
		merkle_size:   merkle_size,
		merkle_data:   merkle_data,
		bf_pb_size:    bf_pb_size,
		bf_bp_bytes:   bf_bp_bytes,
	}

	return md, nil
}

func (mr *MultiRetriever) GetPrefixEntries(key string) (bool, error) {
	if len(mr.sstablePaths) == 0 {
		return false, fmt.Errorf("no SSTable files found")
	}

	mr.currentIndex = 0
	mr.fileReader.ResetReader(mr.sstablePaths[mr.currentIndex], false)

	for {
		fmt.Printf("Searching in SSTable: %s\n", mr.sstablePaths[mr.currentIndex])
		mr.fileReader.SetDirection(false)

		md, err := mr.deserializeMetadata(key)
		if err != nil {
			fmt.Printf("Error deserializing metadata in %s: %v\n", mr.sstablePaths[mr.currentIndex], err)
			if !mr.resetToNextSSTable() {
				return false, fmt.Errorf("key %s not found in any SSTable", key)
			}
			continue
		}

		sumArray, errSum := mr.deserializeSummary(md)
		if errSum != nil {
			fmt.Printf("Error deserializing summary in %s: %v\n", mr.sstablePaths[mr.currentIndex], errSum)
			if !mr.resetToNextSSTable() {
				return false, fmt.Errorf("key %s not found in any SSTable", key)
			}
			continue
		}

		found := false
		for i := 0; i < len(sumArray)-1; i++ {
			if key >= sumArray[i].getKey() && key <= sumArray[i+1].getKey() {
				found = true
				// Key found, read the entry from the file
				//search the offsets
				//ending offset is sumArray[i].getOffset()
				//starting offset is sumArray[i+1].getOffset()
				totalBlocks, _ := mr.fileReader.GetFileSizeBlocks()
				endOffset := sumArray[i].getOffset()
				endOffset = int64(totalBlocks) - endOffset
				startOffset := sumArray[i+1].getOffset() + 1
				startOffset = int64(totalBlocks) - startOffset

				offset, err := mr.searchIndex(startOffset, endOffset, key)
				if err != nil {
					fmt.Printf("Error searching index in %s: %v\n", mr.sstablePaths[mr.currentIndex], err)
					break // Break inner loop, try next SSTable
				}

				dataOffset := int64(totalBlocks) - offset - 1
				if offset == 0 {
					dataOffset -= 1
				}
				value, dataErr := mr.searchData(dataOffset, key)
				if dataErr != nil {
					fmt.Printf("Error searching data in %s: %v\n", mr.sstablePaths[mr.currentIndex], dataErr)
					break // Break inner loop, try next SSTable
				}
				fmt.Printf("Retrieved value for key %s: %s from %s\n", key, value, mr.sstablePaths[mr.currentIndex])
				return true, nil // Found the key, return the value
			}
		}

		if !found {
			fmt.Printf("Key %s not found in range in %s\n", key, mr.sstablePaths[mr.currentIndex])
		}

		// Try next SSTable
		if !mr.resetToNextSSTable() {
			return false, fmt.Errorf("key %s not found in any SSTable", key)
		}
	}
}

func (mr *MultiRetriever) deserializeSummary(metadata Metadata) ([]KeyOffset, error) {
	sortedSummaryArray := make([]KeyOffset, 0, metadata.GetNumOfItems())
	i := metadata.summary_start

	for i < metadata.summary_end {
		offsetInBlock := 0

		data, readBlocks, err := mr.fileReader.ReadEntry(int(i))
		if err != nil {
			return nil, err
		}
		subArray := make([]KeyOffset, 0, metadata.GetNumOfItems())
		for offsetInBlock < len(data) {
			val, offset, off, errorSum := readSummaryIndexEntry(data[offsetInBlock:])
			offsetInBlock += off
			subArray = append(subArray, KeyOffset{key: val, offset: offset})
			if errorSum != nil {
				return nil, fmt.Errorf("error reading summary entry: %v", errorSum)
			}

		}
		sortedSummaryArray = append(subArray, sortedSummaryArray...)
		i += int64(readBlocks)
	}
	return sortedSummaryArray, nil
}
