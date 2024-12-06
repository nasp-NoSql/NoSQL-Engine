package block_manager

import (
	"fmt"
	"math"
	"os"

	"github.com/google/uuid"
)

// BlockManager is an interface for block manager
type blockManager struct {
	BLOCKSIZE int
	// File writer
	// File reader
	//fileReader FileReader
	// Block cache
	blockCache *LRUCache
}

func NewManager(size int, cacheCapacity int) *blockManager {
	return &blockManager{
		BLOCKSIZE:  size,
		blockCache: NewLRUCache(cacheCapacity),
	}
}

func generateFileName() string {
	return uuid.New().String()
}

func (bm *blockManager) WriteBlocks(data []byte, filename string) bool {
	numberOfBlocks := int(math.Ceil(float64(len(data)) / float64(bm.BLOCKSIZE)))

	for i := 0; i < numberOfBlocks; i++ {
		offset := i * bm.BLOCKSIZE
		if i*bm.BLOCKSIZE+bm.BLOCKSIZE > len(data) {
			bm.writeBlockToDisk(data[i*bm.BLOCKSIZE:], filename, offset)
			return true
		}
		blockData := data[i*bm.BLOCKSIZE : (i+1)*bm.BLOCKSIZE]
		written := bm.writeBlockToDisk(blockData, filename, offset)
		if !written {
			fmt.Println("Error writing block to disk")
			return false
		}
		bm.blockCache.Put(filename, i, blockData)
	}

	return true
}

func (bm *blockManager) writeBlockToDisk(data []byte, filename string, offset int) bool {
	f, err := os.OpenFile(filename, os.O_WRONLY|os.O_CREATE, 0644)

	if err != nil {
		fmt.Println(err)
		return false
	}
	len, err := f.WriteAt(data, int64(offset))

	if err != nil {

		fmt.Println(err)

		f.Close()

		return false

	}

	defer f.Close()
	fmt.Println(len, "bytes written successfully")

	return true
}

func (bm *blockManager) ReadBlock(blockId int, filename string) ([]byte, error) {

	data, err := bm.blockCache.Get(filename, blockId)

	if err == nil {
		return data, nil
	}

	data, err = bm.ReadFromDisk(blockId, filename)

	if err != nil {
		return nil, err
	}

	return data, nil

}

func (bm *blockManager) ReadFromDisk(blockId int, filename string) ([]byte, error) {

	f, err := os.OpenFile(filename, os.O_RDONLY, 0644)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	blockData := make([]byte, bm.BLOCKSIZE)
	_, err = f.ReadAt(blockData, int64(blockId*bm.BLOCKSIZE))
	if err != nil {
		return nil, err
	}
	bm.blockCache.Put(filename, blockId, blockData)
	return blockData, nil
}
