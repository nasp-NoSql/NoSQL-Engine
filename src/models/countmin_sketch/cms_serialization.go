package countmin_sketch

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
)

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
