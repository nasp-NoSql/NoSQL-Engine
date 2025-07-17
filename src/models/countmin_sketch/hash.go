package countmin_sketch

import (
	"crypto/md5"
	"encoding/binary"
)

type HashWithSeed struct {
	Seed []byte
}

func (h HashWithSeed) Hash(data []byte) uint64 {
	fn := md5.New()
	fn.Write(append(data, h.Seed...))
	return binary.BigEndian.Uint64(fn.Sum(nil))
}

func CreateHashFunctions(d uint) []HashWithSeed {
	hashes := make([]HashWithSeed, d)
	for i := uint(0); i < d; i++ {
		seed := make([]byte, 4)
		binary.BigEndian.PutUint32(seed, uint32(i+1)*0x9e3779b9)
		hashes[i] = HashWithSeed{Seed: seed}
	}
	return hashes
}
