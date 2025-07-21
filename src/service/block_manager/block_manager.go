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
}

func NewBlockManager() *BlockManager {
	return &BlockManager{
		block_size: CONFIG.BlockSize,
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
	return err
}

func (bm *BlockManager) ReadBlock(location string, blockNumber int, direction bool) ([]byte, error) {
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
		// Check if we're trying to read beyond the file
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

	// If we read 0 bytes, we've hit EOF
	if n == 0 {
		return nil, io.EOF
	}

	return buf[:n], nil
}
