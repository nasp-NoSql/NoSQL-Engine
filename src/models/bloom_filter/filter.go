package bloom_filter

import (
	"bytes"
	"encoding/binary"
	"nosqlEngine/src/config"
	"os"
	"path/filepath"
)

var CONFIG = config.GetConfig()

type BloomFilter struct {
	K      int32
	M      int32
	Array  []byte
	Hashes []HashWithSeed
}

func (filter *BloomFilter) calculateParams(expectedElements int, falsePositiveRate float64) {
	m := CalculateM(expectedElements, falsePositiveRate)
	k := CalculateK(expectedElements, m)
	filter.K = int32(k)
	filter.M = int32(m)
}

func NewBloomFilter() *BloomFilter {
	filter := &BloomFilter{}
	filter.calculateParams(CONFIG.BloomFilterExpectedElements, CONFIG.BloomFilterFalsePositiveRate)
	filter.Array = make([]byte, filter.M)
	filter.Hashes = CreateHashFunctions(uint32(filter.K))
	return filter
}

func (filter *BloomFilter) Add(s string) {
	for _, hash := range filter.Hashes {
		hashed_value := hash.Hash([]byte(s))
		index := hashed_value % uint64(filter.M)
		filter.Array[index] = 1
	}
}

func (filter *BloomFilter) AddMultiple(s []string) []byte {
	for i := 0; i < len(s); i++ {
		for _, hash := range filter.Hashes {
			hashed_value := hash.Hash([]byte(s[i]))
			index := hashed_value % uint64(filter.M)
			filter.Array[index] = 1
		}
	}
	return filter.Array
}

func (filter *BloomFilter) GetArray() []byte {
	return filter.Array
}

func GetBloomFilterArray(s []string) []byte {
	m := CalculateM(len(s), 0.01)
	k := CalculateK(len(s), m)
	hashes, _ := GetHashFunctions("storage/hashes.bin", uint32(k))
	array := make([]byte, m)

	for i := 0; i < len(s); i++ {
		for _, hash := range hashes {
			hashed_value := hash.Hash([]byte(s[i]))
			index := hashed_value % uint64(m)
			array[index] = 1
		}
	}
	return array
}

func GetHashFunctions(filename string, k uint32) ([]HashWithSeed, error) {
	var hashfs []HashWithSeed

	file, err := os.Open(filename)

	if err != nil {
		return CreateHashFunctions(k), err
	}

	defer file.Close()

	hashfs = make([]HashWithSeed, k)

	for i := 0; i < int(k); i++ {
		hash := make([]byte, 4)
		if _, err := file.Read(hash); err != nil {
			return hashfs, err
		}
		hashfs[i] = HashWithSeed{hash}
	}

	return hashfs, nil
}

func (filter *BloomFilter) Check(s string) bool {
	for _, hash := range filter.Hashes {
		index := hash.Hash([]byte(s))
		if filter.Array[index%uint64(filter.M)] != 1 {
			return false
		}
	}
	return true
}

func (filter *BloomFilter) SerializeToByteArray() ([]byte, error) {
	var buffer bytes.Buffer

	if err := binary.Write(&buffer, binary.BigEndian, filter.K); err != nil {
		return nil, err
	}

	if err := binary.Write(&buffer, binary.BigEndian, filter.M); err != nil {
		return nil, err
	}

	if err := binary.Write(&buffer, binary.BigEndian, filter.Array); err != nil {
		return nil, err
	}

	for _, hash := range filter.Hashes {
		if err := binary.Write(&buffer, binary.BigEndian, hash.Seed); err != nil {
			return nil, err
		}
	}

	return buffer.Bytes(), nil
}

func (filter *BloomFilter) Serialize(filename string) error {
	data, err := filter.SerializeToByteArray()
	if err != nil {
		return err
	}

	path := filepath.Join("src/models/serialized", filename)
	return os.WriteFile(path, data, 0644)
}

func Deserialize(filename string) (*BloomFilter, error) {
	path := filepath.Join("src/models/serialized", filename)
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	return DeserializeFromByteArray(data)
}

func DeserializeFromByteArray(data []byte) (*BloomFilter, error) {
	filter := &BloomFilter{}
	reader := bytes.NewReader(data)

	if err := binary.Read(reader, binary.BigEndian, &filter.K); err != nil {
		return filter, err
	}

	if err := binary.Read(reader, binary.BigEndian, &filter.M); err != nil {
		return filter, err
	}

	filter.Array = make([]byte, filter.M)
	if err := binary.Read(reader, binary.BigEndian, filter.Array); err != nil {
		return filter, err
	}

	filter.Hashes = make([]HashWithSeed, filter.K)
	for i := 0; i < int(filter.K); i++ {
		hash := make([]byte, 4)
		if err := binary.Read(reader, binary.BigEndian, hash); err != nil {
			return filter, err
		}
		filter.Hashes[i] = HashWithSeed{hash}
	}

	return filter, nil
}
