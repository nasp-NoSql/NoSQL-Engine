package bloom_filter

import (
	"encoding/binary"
	"os"
	"path/filepath"
)

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

func Initialize(expectedElements int, falsePositiveRate float64) *BloomFilter {
	filter := &BloomFilter{}
	filter.calculateParams(expectedElements, falsePositiveRate)
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

func (filter BloomFilter) Serialize(filename string) error {
	path := filepath.Join("src/models/serialized", filename)
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()

	if err := binary.Write(file, binary.BigEndian, int32(filter.K)); err != nil {
		return err
	}

	if err := binary.Write(file, binary.BigEndian, int32(filter.M)); err != nil {
		return err
	}

	if err := binary.Write(file, binary.BigEndian, filter.Array); err != nil {
		return err
	}

	for _, hash := range filter.Hashes {
		if err := binary.Write(file, binary.BigEndian, hash.Seed); err != nil {
			return err
		}
	}

	return nil
}

func Deserialize(filename string) (BloomFilter, error) {
	var filter BloomFilter
	path := filepath.Join("src/models/serialized", filename)
	file, err := os.Open(path)
	if err != nil {
		return filter, err
	}
	defer file.Close()

	if err := binary.Read(file, binary.BigEndian, &filter.K); err != nil {
		return filter, err
	}

	if err := binary.Read(file, binary.BigEndian, &filter.M); err != nil {
		return filter, err
	}

	filter.Array = make([]byte, filter.M)
	if err := binary.Read(file, binary.BigEndian, filter.Array); err != nil {
		return filter, err
	}

	filter.Hashes = make([]HashWithSeed, filter.K)

	for i := 0; i < int(filter.K); i++ {
		hash := make([]byte, 4)
		if err := binary.Read(file, binary.BigEndian, hash); err != nil {
			return filter, err
		}
		filter.Hashes[i] = HashWithSeed{hash}
	}

	return filter, nil
}
