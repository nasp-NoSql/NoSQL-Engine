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
func (r *EntryRetriever) RetrieveEntry(key string) error {
	r.fileReader.SetDirection(false) // Set to read from back
	i := 0
	initial, readBlocks, err := r.fileReader.ReadEntry(i)
	if err != nil {
		return fmt.Errorf("error reading block %d: %v", i, err)
	}
	i += 1
	fmt.Println("Read blocks : ", readBlocks)
	fmt.Println("Block bytes:", initial)
	mdSize := bytesToInt(initial[len(initial)-8:])

	numOfBlocks := mdSize / int64(CONFIG.BlockSize)
	if mdSize%int64(CONFIG.BlockSize) != 0 {
		numOfBlocks++
	}
	completedBlocks := make([]byte, 0, int(numOfBlocks)*CONFIG.BlockSize)

	for i <= int(numOfBlocks) {
		block, readBlocks, err := r.fileReader.Read(i)
		if err != nil {
			return fmt.Errorf("error reading block %d: %v", i, err)
		}
		fmt.Printf("Read block %d, bytes: %d\n", i, len(block))
		fmt.Println("Block bytes:", block)
		completedBlocks = append(block, completedBlocks...)
		if readBlocks == int(numOfBlocks) {
			break
		}
		i += readBlocks + 1
	}

	completedBlocks = append(completedBlocks, initial...)

	fmt.Println("Completed blocks:", completedBlocks)

	bf_size := bytesToInt(completedBlocks[:8])
	fmt.Println("BLOOM FILTER SIZE:", bf_size)

	bf_data := completedBlocks[8 : 8+bf_size]
	fmt.Println("BLOOM FILTER DATA:", bf_data)

	sum_start_offset := bytesToInt(completedBlocks[8+bf_size : 16+bf_size])
	fmt.Println("SUMMARY START OFFSET:", sum_start_offset)

	num_of_items := bytesToInt(completedBlocks[16+bf_size : 24+bf_size])
	fmt.Println("NUMBER OF ITEMS:", num_of_items)

	len_merkle := bytesToInt(completedBlocks[24+bf_size : 32+bf_size])
	fmt.Println("MERKLE SIZE:", len_merkle)

	merkle_data := completedBlocks[32+bf_size : 32+bf_size+len_merkle]
	fmt.Println("MERKLE DATA:", merkle_data)
	// bf_size := bytesToInt(tempData[:8])
	// fmt.Println("BLOOM FILTER SIZE:", bf_size)
	// bf_data := tempData[8 : 8+bf_size]
	// fmt.Println("BLOOM FILTER DATA:", bf_data)
	// summary_start := bytesToInt(tempData[8+bf_size : 16+bf_size])
	// num_of_items := bytesToInt(tempData[16+bf_size : 24+bf_size])
	// len_merkle := bytesToInt(tempData[24+bf_size : 32+bf_size])
	// merkle_data := tempData[32+bf_size : 32+bf_size+len_merkle]
	// fmt.Println("MERKLE DATA:", merkle_data)
	// fmt.Println("NUM OF ITEMS:", num_of_items)
	// fmt.Println("SUMMARY START:", summary_start)

	return nil
}
