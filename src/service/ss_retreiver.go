package service

import (
	"encoding/binary"
	"errors"
)

//flow : data -> index -> summary -> metadata

type SSTableReader struct {
	data []byte
}

func NewSSTableReader(data []byte) *SSTableReader {
	return &SSTableReader{data: data}
}

func (r *SSTableReader) readMetadata() (int64, int64, error) {
	if len(r.data) < 16 {
		return 0, 0, errors.New("invalid SSTable: insufficient data for metadata")
	}
	summaryStart := binary.BigEndian.Uint64(r.data[len(r.data)-16 : len(r.data)-8]) // 8 bytes for summary start offset
	summarySize := binary.BigEndian.Uint64(r.data[len(r.data)-8:])                  //summary is the last element added to metadata, summary size length is 8 bytes
	return int64(summaryStart), int64(summarySize), nil
}

// Search the summary section for the range containing the key
func (r *SSTableReader) searchSummary(key string, summaryStart int64, summarySize int64) (int64, error) {
	pos := summaryStart

	currKey := ""
	indexOffset := summaryStart
	for pos < summaryStart+summarySize {

		keySize := binary.BigEndian.Uint64(r.data[pos : pos+8])
		pos += 8
		currKey = string(r.data[pos : pos+int64(keySize)])
		pos += int64(keySize)
		indexOffset = int64(binary.BigEndian.Uint64(r.data[pos : pos+8]))
		pos += 8

		if currKey >= key {
			return indexOffset, nil

		}
	}

	return 0, errors.New("key not found in summary")
}

// Search the index section for the exact key and retrieve its data offset
// performing a sequential search in the index section
func (r *SSTableReader) searchIndex(key string, indexStart int64) (int64, error) {
	pos := indexStart
	for pos < int64(len(r.data)) {
		keySize := binary.BigEndian.Uint64(r.data[pos : pos+8]) // getting the key size, 8 bytes
		pos += 8
		currKey := string(r.data[pos : pos+int64(keySize)]) // geting the key value, key size bytes
		pos += int64(keySize)
		dataOffset := binary.BigEndian.Uint64(r.data[pos : pos+8]) // getting the data offset, 8 bytes
		pos += 8

		if currKey == key {
			return int64(dataOffset), nil
		}
	}
	return 0, errors.New("key not found in index")
}

// Retrieve the value from the data section using the offset
func (r *SSTableReader) readValue(dataOffset int64) (string, error) {
	if dataOffset >= int64(len(r.data)) {
		return "", errors.New("invalid data offset")
	}
	valueSize := binary.BigEndian.Uint64(r.data[dataOffset : dataOffset+8]) // getting the value size, 8 bytes
	dataOffset += 8
	value := string(r.data[dataOffset : dataOffset+int64(valueSize)]) // getting the value, value size bytes
	return value, nil
}

// awaiting corrections based on the merkel tree and bloom filter implementations, as well as the actual implementation of the write function
func (r *SSTableReader) GetValue(key string) (string, error) {

	// Step 1: Read metadata
	summaryStart, summarySize, err := r.readMetadata()
	if err != nil {
		return "", err
	}

	// Step 2: Search summary section
	indexOffset, err := r.searchSummary(key, summaryStart, summarySize)
	if err != nil {
		return "", err
	}

	// Step 3: Search index section
	dataOffset, err := r.searchIndex(key, indexOffset)
	if err != nil {
		return "", err
	}

	// Step 4: Read value from data section
	return r.readValue(dataOffset)
}
