package service

import (
	"encoding/binary"
	"errors"
	"fmt"
	"nosqlEngine/src/service/block_manager"
	"nosqlEngine/src/service/file_reader"
	"nosqlEngine/src/utils"
)

//flow : data -> index -> summary -> metadata

type SSRetriever struct {
	reader file_reader.FileReader
}

type Metadata struct {
	mt_content    []byte
	summary_start int64
	summary_size  int64
	found_in_bf   bool
}

type BlockRetrieved struct {
	block      []byte
	index      int
	block_size int
	blockId    int
}

func NewMetadata(mt_content []byte, summary_start int64, summary_size int64, found_in_bf bool) *Metadata {
	return &Metadata{mt_content: mt_content, summary_start: summary_start, summary_size: summary_size, found_in_bf: found_in_bf}
}

func NewBlockRetrieved(block []byte, index int, size int, id int) *BlockRetrieved {
	return &BlockRetrieved{block: block, index: index, block_size: size, blockId: id}
}

func (b *BlockRetrieved) SetBlockData(block []byte) {
	b.block = block
}

func (b *BlockRetrieved) SetLastId(id int) {

	b.blockId = id
}

func (b *BlockRetrieved) SetIndex(index int) {
	b.index = index
}

func NewSSRetriever(reader file_reader.FileReader) *SSRetriever {
	return &SSRetriever{reader: reader}
}

func NewSSTableReader(bm block_manager.BlockManager) *SSRetriever {
	return &SSRetriever{reader: file_reader.NewReader(bm)}
}

func bytesToInt(buf []byte) int64 {

	return int64(binary.BigEndian.Uint64(buf))
}

func readBloom(block *BlockRetrieved) (int64, []byte, error) {

	bloom_size := bytesToInt(block.block[len(block.block)-8:])
	block.SetBlockData(block.block[:len(block.block)-8])
	bloom := block.block[int64(len(block.block))-bloom_size:]
	block.SetBlockData(block.block[:int64(len(block.block))-bloom_size])
	return bloom_size, bloom, nil

}

func (r *SSRetriever) readMetadata(path string, block *BlockRetrieved) ([]byte, int64, []byte, int64, int64, int64, int64, error) {
	block_id := 0
	data, err := r.reader.ReadSS(path, block_id)

	if err != nil {
		return nil, -1, nil, -1, -1, -1, -1, err
	}

	meta_size := bytesToInt(data[len(data)-8:])

	block.SetBlockData(data[:len(data)-8])

	bf_size, bf_content, err := readBloom(block)

	if err != nil {
		return nil, -1, nil, -1, -1, -1, -1, err
	}

	block_size := int64(len(block.block))

	meta_size = meta_size - bf_size - 8

	missing_vals := meta_size - block_size

	for missing_vals > 0 {
		block_id++
		data, err = r.reader.ReadSS(path, block_id)

		if err != nil {
			return nil, -1, nil, -1, -1, -1, -1, err
		}

		data = append(data, block.block...)

		block.SetBlockData(data)

		missing_vals = meta_size - int64(len(data))
	}

	block.SetLastId(block_id)

	merkle_size := bytesToInt(block.block[len(block.block)-8:])
	block.SetBlockData(block.block[:len(block.block)-8])
	merkle_content := block.block[int64(len(block.block))-merkle_size:]
	block.SetBlockData(block.block[:int64(len(block.block))-merkle_size])

	index_size := bytesToInt(block.block[len(block.block)-8:])
	block.SetBlockData(block.block[:len(block.block)-8])

	summary_size := bytesToInt(block.block[len(block.block)-8:])
	block.SetBlockData(block.block[:len(block.block)-8])
	summary_start := bytesToInt(block.block[len(block.block)-8:])
	block.SetBlockData(block.block[:len(block.block)-8])

	data_size := bytesToInt(block.block[len(block.block)-8:])
	block.SetBlockData(block.block[:len(block.block)-8])

	return bf_content, bf_size, merkle_content, index_size, summary_size, summary_start, data_size, nil

}

// Search the summary section for the range containing the key
func (r *SSRetriever) searchSummary(key string, summarySize int64, block *BlockRetrieved, path string) (int64, error) {

	missing_vals := summarySize - int64(len(block.block))

	block_id := block.blockId + 1
	for missing_vals > 0 {

		data, err := r.reader.ReadSS(path, block_id)

		if err != nil {
			return 0, err
		}

		block.SetBlockData(append(data, block.block...))

		missing_vals = summarySize - int64(len(block.block))
		block_id++
	}

	block.SetLastId(block_id)

	summaryBites := block.block[int64(len(block.block))-summarySize:]

	block.SetBlockData(block.block[:int64(len(block.block))-summarySize])

	current_pos := int64(0)

	for current_pos < summarySize {

		key_size := bytesToInt(summaryBites[current_pos : current_pos+8])
		current_pos += 8
		curr_key := string(summaryBites[current_pos : current_pos+key_size])
		current_pos += key_size
		index_offset := bytesToInt(summaryBites[current_pos : current_pos+8])
		current_pos += 8

		if curr_key <= key {
			return index_offset, nil
		}
	}

	return 0, errors.New("key not found in summary")

	// pos := summaryStart

	// currKey := ""
	// indexOffset := summaryStart
	// for pos < summaryStart+summarySize {

	// 	keySize := binary.BigEndian.Uint64(data[pos : pos+8])
	// 	pos += 8
	// 	currKey = string(data[pos : pos+int64(keySize)])
	// 	pos += int64(keySize)
	// 	indexOffset = int64(binary.BigEndian.Uint64(data[pos : pos+8]))
	// 	pos += 8

	// 	if currKey >= key {
	// 		return indexOffset, nil

	// 	}
	// }

}

// Search the index section for the exact key and retrieve its data offset
// performing a sequential search in the index section
func (r *SSRetriever) searchIndex(key string, indexOffset int64, block *BlockRetrieved, summaryOffset int64, path string) (int64, error) {

	toIndex := summaryOffset - indexOffset

	missing_vals := toIndex - int64(len(block.block))
	block_id := block.blockId + 1
	for missing_vals > 0 {

		data, err := r.reader.ReadSS(path, block_id)

		if err != nil {
			return 0, err

		}

		block.SetBlockData(append(data, block.block...))

		missing_vals = toIndex - int64(len(block.block))
		block_id++
	}

	block.SetLastId(block_id)

	indexBytes := block.block[int64(len(block.block))-toIndex:]

	block.SetBlockData(block.block[:int64(len(block.block))-toIndex])

	current_pos := int64(0)

	for current_pos < toIndex {

		key_size := bytesToInt(indexBytes[current_pos : current_pos+8])
		current_pos += 8
		curr_key := string(indexBytes[current_pos : current_pos+key_size])
		current_pos += key_size
		data_offset := bytesToInt(indexBytes[current_pos : current_pos+8])
		current_pos += 8

		if curr_key == key {
			return data_offset, nil
		}

	}
	// pos := indexStart
	// for pos < int64(len(data)) {
	// 	keySize := binary.BigEndian.Uint64(data[pos : pos+8]) // getting the key size, 8 bytes
	// 	pos += 8
	// 	currKey := string(data[pos : pos+int64(keySize)]) // geting the key value, key size bytes
	// 	pos += int64(keySize)
	// 	dataOffset := binary.BigEndian.Uint64(data[pos : pos+8]) // getting the data offset, 8 bytes
	// 	pos += 8

	// 	if currKey == key {
	// 		return int64(dataOffset), nil
	// 	}
	// }
	return 0, errors.New("key not found in index")
}

func (r *SSRetriever) searchData(dataOffset int64, indexOffset int64, block *BlockRetrieved, path string, wantedKey string) (string, error) {

	// block data is until the index offset of the wanted key

	toData := indexOffset - dataOffset

	missing_vals := toData - int64(len(block.block))
	block_id := block.blockId + 1
	for missing_vals > 0 {

		data, err := r.reader.ReadSS(path, block_id)

		if err != nil {

			return "", err

		}

		block.SetBlockData(append(data, block.block...))

		missing_vals = toData - int64(len(block.block))
		block_id++
	}

	block.SetLastId(block_id)

	dataBytes := block.block[int64(len(block.block))-toData:]

	block.SetBlockData(block.block[:int64(len(block.block))-toData])

	current_pos := int64(0)
	key_size := bytesToInt(dataBytes[current_pos : current_pos+8])
	current_pos += 8
	key := string(dataBytes[current_pos : current_pos+key_size])
	current_pos += key_size

	if key == wantedKey {
		value_size := bytesToInt(dataBytes[current_pos : current_pos+8])
		current_pos += 8
		value := string(dataBytes[current_pos : current_pos+value_size])
		return value, nil

	}

	return "", errors.New("key not found in data")
}

// Retrieve the value from the data section using the offset
func (r *SSRetriever) readValue(dataOffset int64, data []byte) (string, error) {
	if dataOffset >= int64(len(data)) {
		return "", errors.New("invalid data offset")
	}
	valueSize := binary.BigEndian.Uint64(data[dataOffset : dataOffset+8]) // getting the value size, 8 bytes
	dataOffset += 8
	value := string(data[dataOffset : dataOffset+int64(valueSize)]) // getting the value, value size bytes
	return value, nil
}

func (r *SSRetriever) GetValue(key string, bSize int) (string, error) {
	paths := utils.GetPaths()
	block := NewBlockRetrieved(nil, 0, bSize, 0)
	for _, path := range paths {

		//read metadata
		bf_size, bf_content, merkle_content, index_size, summary_size, summary_start, data_size, err := r.readMetadata(path, block)

		if err != nil {
			return "", err
		}

		fmt.Println("Bloom filter size: ", bf_size)
		fmt.Println("Bloom filter content: ", bf_content)
		fmt.Println("Merkle tree content: ", merkle_content)
		fmt.Println("Index size: ", index_size)
		fmt.Println("Summary size: ", summary_size)
		fmt.Println("Summary start: ", summary_start)
		fmt.Println("Data size: ", data_size)

		//read summary

		index_offset, err := r.searchSummary(key, summary_size, block, path)

		if err != nil {

			return "", err
		}

		//read index

		data_offset, err := r.searchIndex(key, index_offset, block, summary_start, path)

		if err != nil {

			return "", err

		}

		//read data
		value, err := r.searchData(data_offset, index_offset, block, path, key)

		if err != nil {

			return "", err

		}
		return value, nil
	}

	return "", nil

}
