package simhash

import (
	"crypto/md5"
	"encoding/binary"
	"errors"
	"math/bits"
	"os"
)

// SimHash structure holding the generated 64-bit fingerprint
type SimHash struct {
	Hash uint64
}

// hashFeature hashes a feature with md5, returning a 64-bit hash
func hashFeature(feature string) uint64 {
	sum := md5.Sum([]byte(feature))
	return binary.BigEndian.Uint64(sum[:8])
}

// Generate creates a 64-bit SimHash from a slice of features (strings).
// Features can be tokens, shingles, or words depending on your preprocessing.
func (sh *SimHash) Generate(features []string) {
	var v [64]int

	for _, feature := range features {
		h := hashFeature(feature)

		for i := 0; i < 64; i++ {
			if (h>>uint(i))&1 == 1 {
				v[i] += 1
			} else {
				v[i] -= 1
			}
		}
	}

	var hash uint64 = 0
	for i := 0; i < 64; i++ {
		if v[i] > 0 {
			hash |= (1 << uint(i))
		}
	}

	sh.Hash = hash
}

// HammingDistance computes the Hamming distance between two SimHash fingerprints.
func HammingDistance(a, b uint64) int {
	return bits.OnesCount64(a ^ b)
}

// Serialize stores the SimHash fingerprint into a binary file.
func (sh *SimHash) Serialize(filename string) error {
	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	return binary.Write(file, binary.BigEndian, sh.Hash)
}

// Deserialize reads a SimHash fingerprint from a binary file.
func Deserialize(filename string) (SimHash, error) {
	var sh SimHash

	file, err := os.Open(filename)
	if err != nil {
		return sh, err
	}
	defer file.Close()

	err = binary.Read(file, binary.BigEndian, &sh.Hash)
	if err != nil {
		return sh, err
	}

	return sh, nil
}

// Compare checks if two SimHashes are similar under a provided threshold.
// threshold: maximum Hamming distance allowed to consider them similar.
func Compare(sh1, sh2 SimHash, threshold int) (bool, error) {
	if threshold < 0 || threshold > 64 {
		return false, errors.New("threshold must be between 0 and 64")
	}
	distance := HammingDistance(sh1.Hash, sh2.Hash)
	return distance <= threshold, nil
}