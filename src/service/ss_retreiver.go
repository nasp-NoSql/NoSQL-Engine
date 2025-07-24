package service

import (
	"encoding/binary"
	"fmt"
	"nosqlEngine/src/config"
	"nosqlEngine/src/service/file_reader"
)

type EntryRetriever struct {
	fileReader file_reader.FileReader
}

type Metadata struct {
	bf_size       int64
	bf_data       []byte
	summary_start int64
	summary_end   int64
	numOfItems    int64
	merkle_size   int64
	merkle_data   []byte
}

type Block struct {
	size        int64
	data        []byte
	jumboBuffer []byte
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
func NewEntryRetriever(fileReader file_reader.FileReader) *EntryRetriever {
	return &EntryRetriever{fileReader: fileReader}
}
func (r *EntryRetriever) RetrieveEntry(key string) (bool, error) {
	r.fileReader.SetDirection(false) // Set to read from back

	//we have to retrieve summary data
	md, err := r.deserializeMetadata()
	if err != nil {
		return false, fmt.Errorf("error deserializing metadata: %v", err)
	}

	// Deserialize summary data
	sumArray, errSum := r.deserializeSummary(md)
	if errSum != nil {
		return false, fmt.Errorf("error deserializing summary: %v", errSum)
	}

	// Take each pair from the summary and check if the key is between those 2 or not
	for i := 0; i < len(sumArray)-1; i++ {
		if key >= sumArray[i].getKey() && key <= sumArray[i+1].getKey() {
			fmt.Printf("Key %s is between %s and %s\n", key, sumArray[i].getKey(), sumArray[i+1].getKey())
			// Key found, read the entry from the file
			//search the offsets
			//ending offset is sumArray[i].getOffset()
			//starting offset is sumArray[i+1].getOffset()
			totalBlocks, _ := r.fileReader.GetFileSizeBlocks()
			endOffset := sumArray[i].getOffset()
			endOffset = int64(totalBlocks) - endOffset
			startOffset := sumArray[i+1].getOffset()
			startOffset = int64(totalBlocks) - startOffset
			fmt.Printf("Total blocks: %d, end: %d, start offset: %d\n", totalBlocks, endOffset, startOffset)

			offset, err := r.searchIndex(startOffset, endOffset, key)
			if err != nil {
				return false, fmt.Errorf("error searching index: %v", err)
			}
			fmt.Printf("Found key %s at offset %d\n", key, offset)

			dataOffset := int64(totalBlocks) - offset
			if offset == 0 {
				dataOffset -= 1
			}
			fmt.Printf("Total blocks: %d, Offset: %d, Data offset: %d\n", totalBlocks, offset, dataOffset)
			value, dataErr := r.searchData(dataOffset, key)
			if dataErr != nil {
				return false, fmt.Errorf("error searching data: %v", dataErr)
			}
			fmt.Printf("Retrieved value for key %s: %s\n", key, value)
			return true, nil // Found the key, return the value
			// Now read the entry at the found offset
		}
	}

	return true, nil
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

func (r *EntryRetriever) deserializeMetadata() (Metadata, error) {

	i := 0
	initial, readBlocks, err := r.fileReader.ReadEntry(i)
	if err != nil {
		return Metadata{}, fmt.Errorf("error reading block %d: %v, READ %d blocks", i, err, readBlocks)
	}
	i += 1

	mdSize := bytesToInt(initial[len(initial)-8:])

	numOfBlocks := mdSize / int64(CONFIG.BlockSize)
	if mdSize%int64(CONFIG.BlockSize) != 0 {
		numOfBlocks++
	}

	numOfBlocks = numOfBlocks + 1 // +1 for the initial metadata block
	if mdSize < int64(CONFIG.BlockSize) {
		numOfBlocks = 0 // At least one block for metadata
	}
	completedBlocks := make([]byte, 0, int(numOfBlocks)*CONFIG.BlockSize)

	for i <= int(numOfBlocks) {
		block, readBlocks, err := r.fileReader.Read(i)
		if err != nil {
			return Metadata{}, fmt.Errorf("error reading block %d: %v", i, err)
		}
		completedBlocks = append(block, completedBlocks...)
		if readBlocks == int(numOfBlocks) {
			break
		}
		i += int(readBlocks)
	}

	completedBlocks = append(completedBlocks, initial...)

	bf_size := bytesToInt(completedBlocks[:8])

	bf_data := completedBlocks[8 : 8+bf_size]

	// b, err := bloom_filter.DeserializeFromByteArray(bf_data)

	// if err != nil {
	// 	return fmt.Errorf("error deserializing bloom filter: %v", err)
	// }

	// ex := b.Check(key)

	// fmt.Println("Bloom filter check result for key:", key, "is", ex)

	sum_start_offset := bytesToInt(completedBlocks[8+bf_size : 16+bf_size])

	sum_end_offset := bytesToInt(completedBlocks[16+bf_size : 24+bf_size])
	blocksInFile, err := r.fileReader.GetFileSizeBlocks()
	if err != nil {
		return Metadata{}, fmt.Errorf("error getting file size blocks: %v", err)
	}
	sum_start_offset = int64(blocksInFile) - sum_start_offset
	sum_end_offset = int64(blocksInFile) - sum_end_offset

	numOfItems := bytesToInt(completedBlocks[24+bf_size : 32+bf_size])

	merkle_size := bytesToInt(completedBlocks[32+bf_size : 40+bf_size])
	merkle_data := completedBlocks[40+bf_size : 40+bf_size+merkle_size]

	md := Metadata{
		bf_size:       bf_size,
		bf_data:       bf_data,
		summary_start: sum_start_offset,
		summary_end:   sum_end_offset,
		numOfItems:    numOfItems,
		merkle_size:   merkle_size,
		merkle_data:   merkle_data,
	}

	fmt.Printf("Retrieved metadata: %+v\n", md)
	return md, nil
}

func (r *EntryRetriever) deserializeSummary(metadata Metadata) ([]KeyOffset, error) {
	sortedSummaryArray := make([]KeyOffset, 0, metadata.GetNumOfItems())
	i := metadata.summary_start

	for i < metadata.summary_end {
		offsetInBlock := 0

		data, readBlocks, err := r.fileReader.ReadEntry(int(i))
		fmt.Printf("Reading summary block %d with len %d\n", i, len(data))
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

			fmt.Printf("Retrieved summary block %d data: %s, offset: %d,\n", i, val, offset)
		}
		sortedSummaryArray = append(subArray, sortedSummaryArray...)
		i += int64(readBlocks)
	}
	fmt.Printf("Sorted summary array: %+v\n", sortedSummaryArray)
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
	fmt.Printf("Searching index from %d to %d for key %s\n", start, end, key)
	if start == end {
		start = start - 1
	}
	i := start
	for i < end {
		data, readBlocks, err := r.fileReader.ReadEntry(int(i))
		fmt.Printf("Reading index block %d with len %d\n", i, len(data))
		fmt.Printf("Data in index block %d: %s\n", i, data)
		if err != nil {
			return 0, fmt.Errorf("error reading block %d: %v", i, err)
		}

		offsetInBlock := 0
		for offsetInBlock < len(data) {
			val, offset, off, err := readSummaryIndexEntry(data[offsetInBlock:])
			offsetInBlock += off
			if err != nil {
				return 0, fmt.Errorf("error reading summary entry: %v", err)
			}
			if val == key {
				return offset, nil // Found the key, return the offset
			}
		}
		i += int64(readBlocks)
	}

	return 0, fmt.Errorf("key %s not found in index", key)

}

func (r *EntryRetriever) searchData(offset int64, key string) (string, error) {
	data, _, err := r.fileReader.ReadEntry(int(offset))
	fmt.Printf("BLOCK %d: %s\n", offset, data)
	if err != nil {
		return "", fmt.Errorf("error reading data at offset %d: %v", offset, err)
	}
	offsetInBlock := 0
	for offsetInBlock < len(data) {
		keyRetrieved, value, off, err := readDataEntry(data[offsetInBlock:])
		offsetInBlock += off
		if err != nil {
			return "", fmt.Errorf("error reading summary entry: %v", err)
		}
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
