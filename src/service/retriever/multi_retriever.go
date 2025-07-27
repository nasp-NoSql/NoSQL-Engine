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
	fmt.Print("MULTI Bloom Filter Bytes: ", bf_data, "\n")
	fmt.Print("MULTI Bloom Filter Prefix Bytes: ", len(bf_bp_bytes), "\n")
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
		num_of_items:  numOfItems,
		merkle_size:   merkle_size,
		merkle_data:   merkle_data,
		bf_pb_size:    bf_pb_size,
		bf_bp_bytes:   bf_bp_bytes,
	}

	return md, nil
}

func linearSearch(arr []KeyOffset, prefix string) int {
	for _, item := range arr {
		if item.getKey() == prefix || (len(item.getKey()) >= len(prefix) && item.getKey()[:len(prefix)] == prefix) {
			return int(item.getOffset())
		}
	}
	return -1
}

func binarySearch(arr []KeyOffset, prefix string) int {
	low, high := 0, len(arr)-1

	if arr[0].getKey() > prefix {
		return -1
	}

	if arr[1].getKey() > prefix {
		return int(arr[0].getOffset())
	}

	if arr[len(arr)-1].getKey() < prefix {
		return -1
	}
	ret := -1

	for low <= high {
		mid := (low + high) / 2
		compareTo := arr[mid].getKey()
		offset := int(arr[mid].getOffset())
		if len(compareTo) > len(prefix) {
			compareTo = compareTo[0:len(prefix)]
		}
		if compareTo == prefix {
			return offset
		} else if compareTo < prefix {
			ret = offset
			low = mid + 1
		} else {
			high = mid - 1
		}
	}
	return ret // Key not found
}

func (mr *MultiRetriever) GetPrefixEntries(prefix string) ([]string, error) {
	if len(mr.sstablePaths) == 0 {
		return nil, fmt.Errorf("no SSTable files found")
	}

	mr.currentIndex = 0
	mr.fileReader.ResetReader(mr.sstablePaths[mr.currentIndex], false)
	all_values := make([]string, 0)

	for {
		fmt.Printf("Searching in SSTable: %s\n", mr.sstablePaths[mr.currentIndex])
		mr.fileReader.SetDirection(false)

		md, err := mr.deserializeMetadata(prefix)
		if err != nil {
			fmt.Printf("Error deserializing metadata in %s: %v\n", mr.sstablePaths[mr.currentIndex], err)
			if !mr.resetToNextSSTable() {
				return nil, fmt.Errorf("key %s not found in any SSTable", prefix)
			}
			continue
		}

		sumArray, errSum := mr.deserializeSummary(md)
		fmt.Printf("Summary array for prefix %s: %v\n", prefix, sumArray)
		startingOffset := linearSearch(sumArray, prefix)
		fmt.Printf("Starting offset for prefix %s: %d\n", prefix, startingOffset)
		endingOffset := sumArray[len(sumArray)-1].getOffset()
		if startingOffset == -1 {
			fmt.Printf("Key with prefix %s not found in summary in %s\n", prefix, mr.sstablePaths[mr.currentIndex])
		}

		if errSum != nil {
			fmt.Printf("Error deserializing summary in %s: %v\n", mr.sstablePaths[mr.currentIndex], errSum)
			if !mr.resetToNextSSTable() {
				return nil, fmt.Errorf("key %s not found in any SSTable", prefix)
			}
			continue
		}
		//totalBlocks, err := (mr.fileReader.GetFileSizeBlocks())
		fmt.Printf("Starting offset: %d, Ending offset: %d\n", startingOffset, endingOffset)
		offsets, err := mr.searchIndex(int64(startingOffset), endingOffset, prefix)
		if err != nil {
			fmt.Printf("Error searching index in %s: %v\n", mr.sstablePaths[mr.currentIndex], err)
			break // Break inner loop, try next SSTable
		}
		fmt.Printf("Offsets found for prefix %s: %v\n", prefix, offsets)

		mr.fileReader.SetDirection(true)
		for _, dataOffset := range offsets {
			value, dataErr := mr.searchData(dataOffset, prefix)
			all_values = append(all_values, value)
			if dataErr != nil {
				fmt.Printf("Error searching data in %s: %v\n", mr.sstablePaths[mr.currentIndex], dataErr)
				break // Break inner loop, try next SSTable
			}
			fmt.Printf("Retrieved value for prefix %s: %s from %s\n", prefix, value, mr.sstablePaths[mr.currentIndex])
		}
		// Found the key, return the value
		// Try next SSTable
		if !mr.resetToNextSSTable() {
			return all_values, nil
		}
	}
	fmt.Println(all_values)
	return all_values, nil
}

func (mr *MultiRetriever) searchData(offset int64, prefix string) (string, error) {
	data, _, err := mr.fileReader.ReadEntry(int(offset))
	if err != nil {
		return "", fmt.Errorf("error reading data at offset %d: %v", offset, err)
	}
	offsetInBlock := 0
	for offsetInBlock < len(data) {

		keyRetrieved, value, off, err := readDataEntry(data[offsetInBlock:])
		fmt.Print("Key Retrieved: ", keyRetrieved, " Value: ", value, "\n")
		offsetInBlock += off
		if err != nil {
			return "", fmt.Errorf("error reading summary entry: %v", err)
		}
		fmt.Print("checking if prefix matches: ", keyRetrieved, " with prefix: ", prefix, "\n")
		if len(keyRetrieved) >= len(prefix) && keyRetrieved[:len(prefix)] == prefix {
			return value, nil // Found the key, return the offset
		}
	}
	fmt.Printf("Data not found for prefix %s at offset %d\n", prefix, offset)
	return "", fmt.Errorf("data not found for prefix %s at offset %d", prefix, offset)
}

func (mr *MultiRetriever) deserializeSummary(metadata Metadata) ([]KeyOffset, error) {
	sortedSummaryArray := make([]KeyOffset, 0, metadata.Getnum_of_items())
	i := metadata.summary_start

	for i < metadata.summary_end {
		offsetInBlock := 0

		data, readBlocks, err := mr.fileReader.ReadEntry(int(i))
		if err != nil {
			return nil, err
		}
		subArray := make([]KeyOffset, 0, metadata.Getnum_of_items())
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

func (mr *MultiRetriever) searchIndex(start int64, end int64, prefix string) ([]int64, error) {
	if start == end {
		start = start - 1
	}
	i := start
	var offsets []int64
	mr.fileReader.SetDirection(true)
	for i < end {
		data, readBlocks, err := mr.fileReader.ReadEntry(int(i))

		if err != nil {
			return nil, fmt.Errorf("error reading block %d: %v", i, err)
		}

		offsetInBlock := 0
		for offsetInBlock < len(data) {
			val, offset, off, err := readSummaryIndexEntry(data[offsetInBlock:])
			fmt.Print("Val: ", val, " Offset: ", offset, "\n")
			if err != nil {
				return nil, fmt.Errorf("error reading summary entry: %v", err)
			}
			fmt.Print("Val: ", val, " Offset: ", offset, "\n")
			offsetInBlock += off
			if len(val) >= len(prefix) && val[:len(prefix)] == prefix {
				offsets = append(offsets, offset)
			} else {
				return offsets, nil
			}
		}
		i += int64(readBlocks)
	}
	return offsets, nil
}
