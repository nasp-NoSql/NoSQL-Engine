package retriever

import (
	"encoding/binary"
	"fmt"
	"nosqlEngine/src/service/file_reader"
	"os"
	"path/filepath"
	"runtime"
	"strings"
)


type Metadata struct {
	bf_size       int64
	bf_data       []byte
	bf_pb_size    int64
	bf_bp_bytes   []byte
	summary_start int64
	summary_end   int64
	num_of_items    int64
	merkle_size   int64
	merkle_data   []byte
}

type KeyOffset struct {
	key    string
	offset int64
}

func (ko KeyOffset) getKey() string {
	return ko.key
}

func (ko KeyOffset) getOffset() int64 {
	return ko.offset
}
func (metadata *Metadata) GetBloomFilterSize() int64 {
	return metadata.bf_size
}

func (metadata *Metadata) GetSummaryStartOffset() int64 {
	return metadata.summary_start
}

func (metadata *Metadata) GetSummaryEndOffset() int64 {
	return metadata.summary_end
}

func (metadata *Metadata) Getnum_of_items() int64 {
	return metadata.num_of_items
}
func (metadata *Metadata) GetMerkleSize() int64 {
	return metadata.merkle_size
}
func (metadata *Metadata) GetMerkleData() []byte {
	return metadata.merkle_data
}
func (metadata *Metadata) GetBloomFilter() []byte {
	return metadata.bf_data
}
func deserializeMetadataOnly(reader file_reader.FileReader) (Metadata, error) {
	i := 0
	initial, readBlocks, err := reader.ReadEntry(i)
	if err != nil {
		return Metadata{}, fmt.Errorf("error reading block %d: %v, READ %d blocks", i, err, readBlocks)
	}
	i += 1

	mdOffset := bytesToInt(initial[len(initial)-8:])

	totalBlocks, err := reader.GetFileSizeBlocks()
	if err != nil {
		return Metadata{}, fmt.Errorf("error getting file size blocks: %v", err)
	}
	
	numOfBlocks := int64(totalBlocks) - mdOffset
	completedBlocks := make([]byte, 0, int(numOfBlocks)*CONFIG.BlockSize)
	completedBlocks = append(completedBlocks, initial...)
	for i < int(numOfBlocks) {
		block, readBlocks, err := reader.ReadEntry(i)
		if err != nil {
			return Metadata{}, fmt.Errorf("error reading block %d: %v", i, err)
		}
		completedBlocks = append(block, completedBlocks...)
		if readBlocks == int(numOfBlocks) {
			break
		}
		i += int(readBlocks)
	}
	offsetInBlock := int64(0)

	completedBlocks = append(completedBlocks, initial...)
	bf_size := bytesToInt(completedBlocks[:8])
	offsetInBlock += 8
	bf_data := completedBlocks[offsetInBlock : offsetInBlock+bf_size]
	offsetInBlock += bf_size
	//fmt.Print("Bloom filter size: ", bf_size, "\n")
	bf_pd_size := bytesToInt(completedBlocks[offsetInBlock : offsetInBlock+8])
	offsetInBlock += 8
	bf_bp_bytes := completedBlocks[offsetInBlock : offsetInBlock+bf_pd_size]
	offsetInBlock += bf_pd_size

	sum_start_offset := bytesToInt(completedBlocks[offsetInBlock : offsetInBlock+8])
	offsetInBlock += 8

	sum_end_offset := bytesToInt(completedBlocks[offsetInBlock : offsetInBlock+8])
	offsetInBlock += 8
	blocksInFile, err := reader.GetFileSizeBlocks()
	if err != nil {
		return Metadata{}, fmt.Errorf("error getting file size blocks: %v", err)
	}
	sum_start_offset = int64(blocksInFile) - sum_start_offset
	sum_end_offset = int64(blocksInFile) - sum_end_offset

	num_of_items := bytesToInt(completedBlocks[offsetInBlock : offsetInBlock+8])
	offsetInBlock += 8

	merkle_size := bytesToInt(completedBlocks[offsetInBlock : offsetInBlock+8])
	offsetInBlock += 8
	merkle_data := completedBlocks[offsetInBlock : offsetInBlock+merkle_size]
	
	md := Metadata{
		bf_size:       bf_size,
		bf_data:       bf_data,
		bf_pb_size:    bf_pd_size,
		bf_bp_bytes:   bf_bp_bytes,
		summary_start: sum_start_offset,
		summary_end:   sum_end_offset,
		num_of_items:    num_of_items,
		merkle_size:   merkle_size,
		merkle_data:   merkle_data,
	}

	return md, nil
}
func getProjectRoot() string {
	_, filename, _, _ := runtime.Caller(0)
	// Go up from src/service/file_writer/writer.go to project root
	projectRoot := filepath.Dir(filepath.Dir(filepath.Dir(filepath.Dir(filename))))
	return projectRoot
}
func getFilesFromLevel(level int) []string {
	var sstablePaths []string

	sstableDir := filepath.ToSlash(filepath.Join(getProjectRoot(), "data/sstable"))
	sstablePaths = make([]string, 0)

	files, _ := os.ReadDir(sstableDir + "/lvl" + fmt.Sprint(level))
	for _, file := range files {
		if !strings.HasSuffix(file.Name(), ".db") {
			continue
		}
		sstablePaths = append(sstablePaths, filepath.Join(sstableDir+"/lvl"+fmt.Sprint(level), file.Name()))
	}

	return sstablePaths
}

func bytesToInt(buf []byte) int64 {
	return int64(binary.BigEndian.Uint64(buf))

}