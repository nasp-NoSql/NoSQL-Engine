package retriever

import (
	"encoding/binary"
	"fmt"
	"nosqlEngine/src/config"
	"nosqlEngine/src/models/bloom_filter"
	"nosqlEngine/src/service/block_manager"
	"nosqlEngine/src/service/file_reader"
	"os"
	"path/filepath"
	"strings"
)

type EntryRetriever struct {
	fileReader   file_reader.FileReader
	sstablePaths []string
	currentIndex int
}

type EntryRetrieverPool struct {
	fileReaders    []file_reader.FileReader
	sstablePaths   []string
	currentIndex   int
	metadata       []Metadata
	readCounters   []int64  // Track read values per reader
	currentBlocks  []int64  // Current block index for each reader
	blockPositions []int    // Current position within block for each reader
	cachedBlocks   [][]byte // Cached cleaned block data for each reader
}

type Metadata struct {
	bf_size       int64
	bf_data       []byte
	bf_pb_size    int64
	bf_bp_bytes   []byte
	summary_start int64
	summary_end   int64
	numOfItems    int64
	merkle_size   int64
	merkle_data   []byte
}

type Block struct {
	// Placeholder struct - can be removed if not needed
}

type KeyOffset struct {
	key    string
	offset int64
}

func (ko KeyOffset) getKey() string {
	return ko.key
}

func (ko KeyOffset) getOffset() int64 {
	return ko.offset
}

var CONFIG = config.GetConfig()

func bytesToInt(buf []byte) int64 {
	return int64(binary.BigEndian.Uint64(buf))

}
func NewEntryRetriever(bm *block_manager.BlockManager) *EntryRetriever {
	// Initialize SSTable pool by scanning for sstable files
	sstablePaths := initializeSSTablePool()

	// Create a single block manager and file reader instance
	var fileReader file_reader.FileReader

	if len(sstablePaths) > 0 {
		// Initialize with the first SSTable if available
		fileReader = *file_reader.NewFileReader(sstablePaths[0], CONFIG.BlockSize, *bm)
	} else {
		// Initialize with empty path if no SSTables found
		fileReader = *file_reader.NewFileReader("", CONFIG.BlockSize, *bm)
	}

	return &EntryRetriever{
		fileReader:   fileReader,
		sstablePaths: sstablePaths,
		currentIndex: 0,
	}
}

func NewEntryRetrieverPool(bm *block_manager.BlockManager) *EntryRetrieverPool {
	sstablePaths := initializeSSTablePool()

	fileReaders := make([]file_reader.FileReader, len(sstablePaths))
	readersPerMetadata := make([]Metadata, len(sstablePaths))
	readCounters := make([]int64, len(sstablePaths))
	currentBlocks := make([]int64, len(sstablePaths))
	blockPositions := make([]int, len(sstablePaths))
	cachedBlocks := make([][]byte, len(sstablePaths))

	for i, sstablePath := range sstablePaths {
		fileReaders[i] = *file_reader.NewFileReader(sstablePath, CONFIG.BlockSize, *bm)
		md, err := deserializeMetadataOnly(fileReaders[i])
		if err != nil {
			fmt.Printf("Error deserializing metadata for %s: %v\n", sstablePath, err)
			readersPerMetadata[i] = Metadata{}
		} else {
			readersPerMetadata[i] = md
		}
		readCounters[i] = 0 // Initialize read counter

		// Initialize reading state - start from the beginning of data section
		totalBlocks, _ := fileReaders[i].GetFileSizeBlocks()
		currentBlocks[i] = int64(totalBlocks) - readersPerMetadata[i].summary_start // Start after summary
		blockPositions[i] = 0                                                       // Start at beginning of block
		cachedBlocks[i] = nil                                                       // No cached block initially
	}

	return &EntryRetrieverPool{
		fileReaders:    fileReaders,
		sstablePaths:   sstablePaths,
		currentIndex:   0,
		metadata:       readersPerMetadata,
		readCounters:   readCounters,
		currentBlocks:  currentBlocks,
		blockPositions: blockPositions,
		cachedBlocks:   cachedBlocks,
	}
}

func (r *EntryRetrieverPool) ReadNextVal(readerIndex int) (string, string, bool, error) {
	if readerIndex < 0 || readerIndex >= len(r.fileReaders) {
		return "", "", false, fmt.Errorf("reader index %d out of range [0, %d)", readerIndex, len(r.fileReaders))
	}

	// Check if we've reached the limit for this reader
	if r.readCounters[readerIndex] >= r.metadata[readerIndex].GetNumOfItems() {
		return "", "", true, nil // Return flag indicating limit reached
	}

	// Check if we need to load a new block
	if r.cachedBlocks[readerIndex] == nil || r.blockPositions[readerIndex] >= len(r.cachedBlocks[readerIndex]) {
		err := r.loadNextBlock(readerIndex)
		if err != nil {
			return "", "", false, fmt.Errorf("error loading next block: %v", err)
		}
	}

	// Read the next entry from the cached block
	key, value, bytesRead, err := readDataEntry(r.cachedBlocks[readerIndex][r.blockPositions[readerIndex]:])
	if err != nil {
		return "", "", false, fmt.Errorf("error reading data entry: %v", err)
	}

	// Update position and counter
	r.blockPositions[readerIndex] += bytesRead
	r.readCounters[readerIndex]++

	return key, value, false, nil
}

func (r *EntryRetrieverPool) loadNextBlock(readerIndex int) error {
	reader := r.fileReaders[readerIndex]

	// Read the block at current position
	data, readBlocks, err := reader.ReadEntry(int(r.currentBlocks[readerIndex]))
	if err != nil {
		return fmt.Errorf("error reading block %d: %v", r.currentBlocks[readerIndex], err)
	}

	// TODO: Clean the block data (remove <!> markers, handle jumbo blocks, etc.)
	// For now, just cache the raw data
	r.cachedBlocks[readerIndex] = data
	r.blockPositions[readerIndex] = 0
	r.currentBlocks[readerIndex] += int64(readBlocks)

	return nil
}

func initializeSSTablePool() []string {
	var sstablePaths []string

	sstableDir := `..\..\..\data\sstable`

	files, err := os.ReadDir(sstableDir)
	sstablePaths = make([]string, 0, len(files))
	if err != nil {
		fmt.Printf("Error scanning SSTable directory %s: %v\n", sstableDir, err)
		return []string{}
	}

	for _, file := range files {
		if strings.HasSuffix(file.Name(), ".db") && strings.Contains(file.Name(), "sstable_") {
			sstablePaths = append(sstablePaths, filepath.Join(sstableDir, file.Name()))
		}
	}

	fmt.Printf("Found %d SSTable files in %s: %v\n", len(sstablePaths), sstableDir, sstablePaths)
	return sstablePaths
}

func (r *EntryRetriever) resetToNextSSTable() bool {
	r.currentIndex++
	if r.currentIndex >= len(r.sstablePaths) {
		return false // No more SSTables to check
	}

	r.fileReader.ResetReader(r.sstablePaths[r.currentIndex], false)
	fmt.Printf("Switched to SSTable: %s\n", r.sstablePaths[r.currentIndex])
	return true
}

func (r *EntryRetriever) RetrieveEntry(key string) (bool, error) {
	if len(r.sstablePaths) == 0 {
		return false, fmt.Errorf("no SSTable files found")
	}

	r.currentIndex = 0
	r.fileReader.ResetReader(r.sstablePaths[r.currentIndex], false)

	for {
		fmt.Printf("Searching in SSTable: %s\n", r.sstablePaths[r.currentIndex])
		r.fileReader.SetDirection(false)

		md, err := r.deserializeMetadata(key)
		if err != nil {
			fmt.Printf("Error deserializing metadata in %s: %v\n", r.sstablePaths[r.currentIndex], err)
			if !r.resetToNextSSTable() {
				return false, fmt.Errorf("key %s not found in any SSTable", key)
			}
			continue
		}

		sumArray, errSum := r.deserializeSummary(md)
		if errSum != nil {
			fmt.Printf("Error deserializing summary in %s: %v\n", r.sstablePaths[r.currentIndex], errSum)
			if !r.resetToNextSSTable() {
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
				totalBlocks, _ := r.fileReader.GetFileSizeBlocks()
				endOffset := sumArray[i].getOffset()
				endOffset = int64(totalBlocks) - endOffset
				startOffset := sumArray[i+1].getOffset() + 1
				startOffset = int64(totalBlocks) - startOffset

				offset, err := r.searchIndex(startOffset, endOffset, key)
				if err != nil {
					fmt.Printf("Error searching index in %s: %v\n", r.sstablePaths[r.currentIndex], err)
					break // Break inner loop, try next SSTable
				}

				dataOffset := int64(totalBlocks) - offset - 1
				if offset == 0 {
					dataOffset -= 1
				}
				value, dataErr := r.searchData(dataOffset, key)
				if dataErr != nil {
					fmt.Printf("Error searching data in %s: %v\n", r.sstablePaths[r.currentIndex], dataErr)
					break // Break inner loop, try next SSTable
				}
				fmt.Printf("Retrieved value for key %s: %s from %s\n", key, value, r.sstablePaths[r.currentIndex])
				return true, nil // Found the key, return the value
			}
		}

		if !found {
			fmt.Printf("Key %s not found in range in %s\n", key, r.sstablePaths[r.currentIndex])
		}

		// Try next SSTable
		if !r.resetToNextSSTable() {
			return false, fmt.Errorf("key %s not found in any SSTable", key)
		}
	}
}

func (metadata *Metadata) GetBloomFilterSize() int64 {
	return metadata.bf_size
}

func (metadata *Metadata) GetSummaryStartOffset() int64 {
	return metadata.summary_start
}

func (metadata *Metadata) GetSummaryEndOffset() int64 {
	return metadata.summary_end
}

func (metadata *Metadata) GetNumOfItems() int64 {
	return metadata.numOfItems
}
func (metadata *Metadata) GetMerkleSize() int64 {
	return metadata.merkle_size
}
func (metadata *Metadata) GetMerkleData() []byte {
	return metadata.merkle_data
}
func (metadata *Metadata) GetBloomFilter() []byte {
	return metadata.bf_data
}

func (r *EntryRetriever) deserializeMetadata(key string) (Metadata, error) {

	i := 0
	initial, readBlocks, err := r.fileReader.ReadEntry(i)
	if err != nil {
		return Metadata{}, fmt.Errorf("error reading block %d: %v, READ %d blocks", i, err, readBlocks)
	}
	i += 1

	mdOffset := bytesToInt(initial[len(initial)-8:])

	totalBlocks, err := r.fileReader.GetFileSizeBlocks()
	if err != nil {
		return Metadata{}, fmt.Errorf("error getting file size blocks: %v", err)
	}
	numOfBlocks := int64(totalBlocks) - mdOffset
	completedBlocks := make([]byte, 0, int(numOfBlocks)*CONFIG.BlockSize)
	completedBlocks = append(completedBlocks, initial...)
	for i < int(numOfBlocks) {
		block, readBlocks, err := r.fileReader.ReadEntry(i)
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
	b, err := bloom_filter.DeserializeFromByteArray(bf_data)

	bf_pb_size := bytesToInt(completedBlocks[offsetInBlock : offsetInBlock+8])
	offsetInBlock += 8
	bf_bp_bytes := completedBlocks[offsetInBlock : offsetInBlock+bf_pb_size]
	offsetInBlock += bf_pb_size
	if err != nil {
		return Metadata{}, fmt.Errorf("error deserializing bloom filter: %v", err)
	}

	ex := b.Check(key)
	if !ex {
		return Metadata{}, fmt.Errorf("key %s not found in bloom filter", key)
	}

	sum_start_offset := bytesToInt(completedBlocks[offsetInBlock : offsetInBlock+8])
	offsetInBlock += 8

	sum_end_offset := bytesToInt(completedBlocks[offsetInBlock : offsetInBlock+8])
	offsetInBlock += 8
	blocksInFile, err := r.fileReader.GetFileSizeBlocks()
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

func (r *EntryRetriever) deserializeSummary(metadata Metadata) ([]KeyOffset, error) {
	sortedSummaryArray := make([]KeyOffset, 0, metadata.GetNumOfItems())
	i := metadata.summary_start

	for i < metadata.summary_end {
		offsetInBlock := 0

		data, readBlocks, err := r.fileReader.ReadEntry(int(i))
		//this is one summary block which can contain multiple entries
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

func readSummaryIndexEntry(data []byte) (string, int64, int, error) {
	if len(data) < 16 {
		return "", 0, 0, fmt.Errorf("invalid summary entry data")
	}
	off := 0
	valSize := bytesToInt(data[:8])
	off += 8
	val := data[8 : 8+valSize]
	off += int(valSize)
	offset := data[8+valSize : 8+valSize+8]
	off += 8

	return string(val), bytesToInt(offset), off, nil
}

func (r *EntryRetriever) searchIndex(start int64, end int64, key string) (int64, error) {
	if start == end {
		start = start - 1
	}
	i := start
	for i < end {
		data, readBlocks, err := r.fileReader.ReadEntry(int(i))
		if err != nil {
			return 0, fmt.Errorf("error reading block %d: %v", i, err)
		}

		offsetInBlock := 0
		for offsetInBlock < len(data) {
			val, offset, off, err := readSummaryIndexEntry(data[offsetInBlock:])
			if err != nil {
				return 0, fmt.Errorf("error reading summary entry: %v", err)
			}
			fmt.Print("Val: ", val, " Offset: ", offset, "\n")
			offsetInBlock += off
			if val == key {
				return offset, nil
			}
		}
		i += int64(readBlocks)
	}

	return 0, fmt.Errorf("key %s not found in index", key)

}

func (r *EntryRetriever) searchData(offset int64, key string) (string, error) {
	data, _, err := r.fileReader.ReadEntry(int(offset))
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
		fmt.Print("checking if key matches: ", len(keyRetrieved), " with key: ", len(key), "\n")
		if keyRetrieved == key {
			return value, nil // Found the key, return the offset
		}
	}
	fmt.Printf("Data not found for key %s at offset %d\n", key, offset)
	return "", fmt.Errorf("data not found for key %s at offset %d", key, offset)
}

func readDataEntry(data []byte) (string, string, int, error) {
	if len(data) < 16 {
		return "", "", 0, fmt.Errorf("invalid data entry")
	}
	off := 0
	keySize := bytesToInt(data[:8])
	off += 8
	key := data[8 : 8+keySize]
	off += int(keySize)
	valueSize := bytesToInt(data[8+keySize : 16+keySize])
	off += 8
	value := data[16+keySize : 16+keySize+valueSize]
	off += int(valueSize)
	return string(key), string(value), off, nil
}

func deserializeMetadataOnly(reader file_reader.FileReader) (Metadata, error) {
	i := 0
	initial, readBlocks, err := reader.ReadEntry(i)
	if err != nil {
		return Metadata{}, fmt.Errorf("error reading block %d: %v, READ %d blocks", i, err, readBlocks)
	}
	i += 1

	mdOffset := bytesToInt(initial[len(initial)-8:])

	totalBlocks, err := reader.GetFileSizeBlocks()
	if err != nil {
		return Metadata{}, fmt.Errorf("error getting file size blocks: %v", err)
	}
	numOfBlocks := int64(totalBlocks) - mdOffset
	completedBlocks := make([]byte, 0, int(numOfBlocks)*CONFIG.BlockSize)
	completedBlocks = append(completedBlocks, initial...)
	for i < int(numOfBlocks) {
		block, readBlocks, err := reader.ReadEntry(i)
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

	bf_pd_size := bytesToInt(completedBlocks[offsetInBlock : offsetInBlock+8])
	offsetInBlock += 8
	bf_bp_bytes := completedBlocks[offsetInBlock : offsetInBlock+bf_pd_size]
	offsetInBlock += bf_pd_size

	sum_start_offset := bytesToInt(completedBlocks[offsetInBlock : offsetInBlock+8])
	offsetInBlock += 8

	sum_end_offset := bytesToInt(completedBlocks[offsetInBlock : offsetInBlock+8])
	offsetInBlock += 8
	blocksInFile, err := reader.GetFileSizeBlocks()
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
		bf_pb_size:    bf_pd_size,
		bf_bp_bytes:   bf_bp_bytes,
		summary_start: sum_start_offset,
		summary_end:   sum_end_offset,
		numOfItems:    numOfItems,
		merkle_size:   merkle_size,
		merkle_data:   merkle_data,
	}

	return md, nil
}

func loadDataEntry(reader file_reader.FileReader) (int64, string) {
	// TODO: Implement this function based on your requirements
	// This should load the next data entry from the reader
	return 0, ""
}
