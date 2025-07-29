package block_manager

import (
	"fmt"
	"io"
	"nosqlEngine/src/config"
	"os"
)

var CONFIG = config.GetConfig()

type BlockManager struct {
	block_size int
	lruCache   *LRUCache
}

func NewBlockManager() *BlockManager {
	return &BlockManager{
		block_size: CONFIG.BlockSize,
		lruCache:   NewLRUCache(),
	}
}

func (bm *BlockManager) WriteBlock(location string, blockNumber int, data []byte) error {
	if len(data) > bm.block_size {
		return fmt.Errorf("data size exceeds block size")
	}

	file, err := os.OpenFile(location, os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		return err
	}
	defer file.Close()

	offset := int64(bm.block_size * blockNumber)
	_, err = file.Seek(offset, 0)
	if err != nil {
		return err
	}

	_, err = file.Write(data)
	if err != nil {
		return err
	}

	//bm.lruCache.Put(location, blockNumber, data)

	return nil
}

func (bm *BlockManager) ReadBlock(location string, blockNumber int, direction bool) ([]byte, error) {
	var forwardBlockNumber int

	if direction {
		forwardBlockNumber = blockNumber
	} else {
		fileInfo, err := os.Stat(location)
		if err != nil {
			fmt.Println("Error getting file info:", err, " for forwardBlockNumber:", forwardBlockNumber)
			return nil, err
		}
		totalBlocks := int(fileInfo.Size()) / bm.block_size
		if blockNumber >= totalBlocks {
			return nil, io.EOF
		}
		forwardBlockNumber = totalBlocks - 1 - blockNumber
	}

	// if data, err := bm.lruCache.Get(location, forwardBlockNumber); err == nil {
	// 	fmt.Println("Cache hit for block:", forwardBlockNumber)
	// 	return data, nil
	// }

	file, err := os.Open(location)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var offset int64
	fileInfo, err := file.Stat()
	if err != nil {
		return nil, err
	}

	if direction { // true: read from start
		offset = int64(bm.block_size * blockNumber)
		if offset >= fileInfo.Size() {
			return nil, io.EOF
		}
	} else { // false: read from end
		totalBlocks := int(fileInfo.Size()) / bm.block_size
		if blockNumber >= totalBlocks {
			return nil, io.EOF
		}
		offset = int64(bm.block_size * (totalBlocks - 1 - blockNumber))
		if offset < 0 {
			offset = 0
		}
	}

	_, err = file.Seek(offset, 0)
	if err != nil {
		return nil, err
	}
	buf := make([]byte, bm.block_size)
	n, err := file.Read(buf)
	if err != nil {
		return nil, err
	}

	if n == 0 {
		return nil, io.EOF
	}

	data := buf[:n]
	//bm.lruCache.Put(location, forwardBlockNumber, data)
	return data, nil
}

func (bm *BlockManager) GetFileSize(location string) (int64, error) {
	fileInfo, err := os.Stat(location)
	if err != nil {
		return 0, err
	}
	return fileInfo.Size(), nil
}

func (bm *BlockManager) GetFileSizeBlocks(location string) (int, error) {
	size, err := bm.GetFileSize(location)
	if err != nil {
		return 0, err
	}
	if size == 0 {
		return 0, nil
	}
	return int(size) / bm.block_size, nil
}

func (bm *BlockManager) ClearCache() {
	bm.lruCache = NewLRUCache()
}

func (bm *BlockManager) IsCached(location string, blockNumber int) bool {
	_, err := bm.lruCache.Get(location, blockNumber)
	return err == nil
}
