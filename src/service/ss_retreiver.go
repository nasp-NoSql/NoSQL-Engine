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

func NewSSRetriever(reader file_reader.FileReader) *SSRetriever {
	return &SSRetriever{reader: reader}
}

func NewSSTableReader(bm block_manager.BlockManager) *SSRetriever {
	return &SSRetriever{reader: file_reader.NewReader(bm)}
}

func (r *SSRetriever) readMetadata(data []byte) (int64, int64, []byte, []byte, error) {
	if len(data) < 16 {
		return 0, 0, nil, nil, errors.New("invalid SSTable: insufficient data for metadata")
	}
	offset := 0
	bfSize := binary.BigEndian.Uint64(data[offset:8]) // 8 bytes for bloom filter size

	offset += 8
	bfBytes := data[offset : offset+int(bfSize)] // bloom filter bytes

	offset += int(bfSize)

	mtSize := binary.BigEndian.Uint64(data[offset : offset+8]) // 8 bytes for merkle tree size

	offset += 8

	mtBytes := data[offset : offset+int(mtSize)] // merkle tree bytes

	offset += int(mtSize)

	summarySize := binary.BigEndian.Uint64(data[offset : offset+8]) // 8 bytes for summary size

	offset += 8

	summaryStart := binary.BigEndian.Uint64(data[offset : offset+8]) // 8 bytes for summary start offset

	return int64(summaryStart), int64(summarySize), bfBytes, mtBytes, nil
}

// Search the summary section for the range containing the key
func (r *SSRetriever) searchSummary(key string, summaryStart int64, summarySize int64, data []byte) (int64, error) {
	pos := summaryStart

	currKey := ""
	indexOffset := summaryStart
	for pos < summaryStart+summarySize {

		keySize := binary.BigEndian.Uint64(data[pos : pos+8])
		pos += 8
		currKey = string(data[pos : pos+int64(keySize)])
		pos += int64(keySize)
		indexOffset = int64(binary.BigEndian.Uint64(data[pos : pos+8]))
		pos += 8

		if currKey >= key {
			return indexOffset, nil

		}
	}

	return 0, errors.New("key not found in summary")
}

// Search the index section for the exact key and retrieve its data offset
// performing a sequential search in the index section
func (r *SSRetriever) searchIndex(key string, indexStart int64, data []byte) (int64, error) {
	pos := indexStart
	for pos < int64(len(data)) {
		keySize := binary.BigEndian.Uint64(data[pos : pos+8]) // getting the key size, 8 bytes
		pos += 8
		currKey := string(data[pos : pos+int64(keySize)]) // geting the key value, key size bytes
		pos += int64(keySize)
		dataOffset := binary.BigEndian.Uint64(data[pos : pos+8]) // getting the data offset, 8 bytes
		pos += 8

		if currKey == key {
			return int64(dataOffset), nil
		}
	}
	return 0, errors.New("key not found in index")
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

// awaiting corrections based on the merkel tree and bloom filter implementations, as well as the actual implementation of the write function
func (r *SSRetriever) GetValue(key string) (string, error) {
	paths := utils.GetPaths()

	for _, path := range paths {
		metadata, err := r.reader.ReadSS(path, 0)

		sumStart, sumSize, bf, mt, err := r.readMetadata(metadata)

		fmt.Println(sumStart, sumSize, bf, mt)

		if err != nil {
			return "", err
		}
	}

	return "", nil

}
