package bloom_filter

import (
	"encoding/binary"
	"fmt"
	"os"
)

type BloomFilter struct {
	k      int32
	m      int32
	array  []byte
	hashes []HashWithSeed
}

func (filter *BloomFilter) calculateParams(expectedElements int, falsePositiveRate float64) {
	m := CalculateM(expectedElements, falsePositiveRate)
	k := CalculateK(expectedElements, m)
	filter.k = int32(k)
	filter.m = int32(m)
}

func (filter *BloomFilter) Initialize(expectedElements int, falsePositiveRate float64) {
	filter.calculateParams(expectedElements, falsePositiveRate)
	filter.array = make([]byte, filter.m)
	filter.hashes = CreateHashFunctions(uint32(filter.k))
}

func (filter *BloomFilter) Add(s string) {
	for _, hash := range filter.hashes {
		hashed_value := hash.Hash([]byte(s))
		index := hashed_value % uint64(filter.m)
		filter.array[index] = 1
		print(index, " ")
	}
	fmt.Println(filter.array)
}

func (filter *BloomFilter) Check(s string) bool {
	for _, hash := range filter.hashes {
		index := hash.Hash([]byte(s))
		if filter.array[index%uint64(filter.m)] != 1 {
			return false
		}
	}
	return true
}

func (filter BloomFilter) Serialize(filename string) error {
	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	if err := binary.Write(file, binary.BigEndian, int32(filter.k)); err != nil {
		return err
	}

	if err := binary.Write(file, binary.BigEndian, int32(filter.m)); err != nil {
		return err
	}

	if err := binary.Write(file, binary.BigEndian, filter.array); err != nil {
		return err
	}

	for _, hash := range filter.hashes {
		if err := binary.Write(file, binary.BigEndian, hash.Seed); err != nil {
			return err
		}
	}

	return nil
}

func Deserialize(filename string) (BloomFilter, error) {
	var filter BloomFilter

	file, err := os.Open(filename)

	if err != nil {
		return filter, err
	}

	defer file.Close()

	if err := binary.Read(file, binary.BigEndian, &filter.k); err != nil {
		return filter, err
	}

	if err := binary.Read(file, binary.BigEndian, &filter.m); err != nil {
		return filter, err
	}

	filter.array = make([]byte, filter.m)
	if _, err := file.Read(filter.array); err != nil {
		return filter, err
	}

	filter.hashes = make([]HashWithSeed, filter.k)

	for i := 0; i < int(filter.k); i++ {
		hash := make([]byte, 4)
		if _, err := file.Read(hash); err != nil {
			return filter, err
		}
		filter.hashes[i] = HashWithSeed{hash}
	}

	return filter, nil
}
