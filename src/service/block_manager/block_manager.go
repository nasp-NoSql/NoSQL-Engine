package block_manager

import (
	"fmt"
	"os"
)

const BLOCK_SIZE = 40 // Adjust as needed

type BlockManager struct{}

func NewBlockManager() *BlockManager {
	return &BlockManager{}
}

func (bm *BlockManager) WriteBlock(location string, blockNumber int, data []byte) error {
	if len(data) > BLOCK_SIZE {
		return fmt.Errorf("data size exceeds BLOCK_SIZE")
	}
	file, err := os.OpenFile(location, os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		return err
	}
	defer file.Close()

	offset := int64(BLOCK_SIZE * blockNumber)
	_, err = file.Seek(offset, 0)
	if err != nil {
		return err
	}
	padded := make([]byte, BLOCK_SIZE)
	copy(padded, data)
	_, err = file.Write(padded)
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
		offset = int64(BLOCK_SIZE * blockNumber)
	} else { // false: read from end
		totalBlocks := int(fileInfo.Size()) / BLOCK_SIZE
		offset = int64(BLOCK_SIZE * (totalBlocks - 1 - blockNumber))
		if offset < 0 {
			offset = 0
		}
	}
	_, err = file.Seek(offset, 0)
	if err != nil {
		return nil, err
	}
	buf := make([]byte, BLOCK_SIZE)
	n, err := file.Read(buf)
	if err != nil {
		return nil, err
	}
	return buf[:n], nil
}
