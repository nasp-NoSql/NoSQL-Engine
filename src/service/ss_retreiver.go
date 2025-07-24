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

var CONFIG = config.GetConfig()

func bytesToInt(buf []byte) int64 {
	return int64(binary.BigEndian.Uint64(buf))

}
func NewEntryRetriever(fileReader file_reader.FileReader) *EntryRetriever {
	return &EntryRetriever{fileReader: fileReader}
}
func (r *EntryRetriever) RetrieveEntry(key string) (Metadata, error) {
	r.fileReader.SetDirection(false) // Set to read from back
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
		i += readBlocks
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
	//we have to retrieve summary data

	for i := md.summary_start; i < md.summary_end; i++ {
		dataWithNotation, readBlocks, err := r.fileReader.ReadEntry(int(i))
		valSize := bytesToInt(dataWithNotation[:8])
		val := dataWithNotation[8 : 8+valSize]
		offset := dataWithNotation[8+valSize : 8+valSize+8]
		fmt.Printf("Retrieved summary block %d data: %s, offset: %d,\n", i, val, bytesToInt(offset))
		if err != nil {
			return Metadata{}, fmt.Errorf("error reading summary block %d: %v, READ %d blocks", i, err, readBlocks)
		}
		fmt.Printf("Retrieved summary block %d data: %v, READ %d blocks\n", i, dataWithNotation, readBlocks)

	}
	return md, nil
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
