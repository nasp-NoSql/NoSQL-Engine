package countmin_sketch

import (
	"crypto/md5"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"os"
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

type CountMinSketch struct {
	w      uint
	d      uint
	table  [][]uint
	hashes []HashWithSeed
}

// CalculateW returns width for desired epsilon
func CalculateW(epsilon float64) uint {
	return uint(math.Ceil(math.E / epsilon)) // 2.72/0.000001 broj kolona
}

// CalculateD returns depth for desired delta
func CalculateD(delta float64) uint {
	return uint(math.Ceil(math.Log(1 / delta))) // broj heševa i broj redova
}

// Initialize creates a new CMS with error rate epsilon, confidence 1-delta
func (cms *CountMinSketch) Initialize(epsilon, delta float64) {
	cms.w = CalculateW(epsilon)
	cms.d = CalculateD(delta)

	cms.table = make([][]uint, cms.d)
	for i := range cms.table {
		cms.table[i] = make([]uint, cms.w)
	}

	cms.hashes = CreateHashFunctions(cms.d)
}

// Add increments counters for the data
func (cms *CountMinSketch) Add(data []byte) {
	for i := uint(0); i < cms.d; i++ {
		hashVal := cms.hashes[i].Hash(data)
		idx := hashVal % uint64(cms.w)
		cms.table[i][idx]++
	}
}

// Estimate returns approximate frequency count of the data
func (cms *CountMinSketch) Estimate(data []byte) uint {
	min := ^uint(0) // max uint
	for i := uint(0); i < cms.d; i++ {
		hashVal := cms.hashes[i].Hash(data)
		idx := hashVal % uint64(cms.w)
		if cms.table[i][idx] < min {
			min = cms.table[i][idx]
		}
	}
	return min
}

func (cms *CountMinSketch) Serialize(filename string) error {
	f, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer f.Close()

	// Header
	if _, err := f.Write([]byte("CMS1")); err != nil {
		return err
	}
	if err := binary.Write(f, binary.BigEndian, uint32(1)); err != nil { // Version
		return err
	}
	if err := binary.Write(f, binary.BigEndian, uint32(cms.w)); err != nil {
		return err
	}
	if err := binary.Write(f, binary.BigEndian, uint32(cms.d)); err != nil {
		return err
	}

	// Data
	for i := uint(0); i < cms.d; i++ {
		for j := uint(0); j < cms.w; j++ {
			if err := binary.Write(f, binary.BigEndian, cms.table[i][j]); err != nil {
				return err
			}
		}
	}
	return nil
}

func (cms *CountMinSketch) Deserialize(filename string) error {
	f, err := os.Open(filename)
	if err != nil {
		return err
	}
	defer f.Close()

	header := make([]byte, 4)
	if _, err := io.ReadFull(f, header); err != nil {
		return err
	}
	if string(header) != "CMS1" {
		return fmt.Errorf("invalid CMS file header")
	}

	var version uint32
	if err := binary.Read(f, binary.BigEndian, &version); err != nil {
		return err
	}
	if version != 1 {
		return fmt.Errorf("unsupported CMS version: %d", version)
	}

	var w32, d32 uint32
	if err := binary.Read(f, binary.BigEndian, &w32); err != nil {
		return err
	}
	if err := binary.Read(f, binary.BigEndian, &d32); err != nil {
		return err
	}
	cms.w = uint(w32)
	cms.d = uint(d32)

	// Allocate table
	cms.table = make([][]uint, cms.d)
	for i := uint(0); i < cms.d; i++ {
		cms.table[i] = make([]uint, cms.w)
		for j := uint(0); j < cms.w; j++ {
			if err := binary.Read(f, binary.BigEndian, &cms.table[i][j]); err != nil {
				return err
			}
		}
	}

	// Re-create hashes deterministically
	cms.hashes = CreateHashFunctions(cms.d)

	return nil
}

func Deserialize(filename string) (*CountMinSketch, error) {
	cms := &CountMinSketch{}
	err := cms.Deserialize(filename)
	if err != nil {
		return nil, err
	}
	return cms, nil
}