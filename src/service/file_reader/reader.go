package file_reader

import "nosqlEngine/src/service/block_manager"

type FileReader struct {
	block_manager   block_manager.BlockManager
	location        string
	currentBlock    []byte
	currentBlockNum int
	blockSize       int
	offsetInBlock   int
	allDataRead     []byte
	direction       bool // true for forward, false for backward
}

func NewFileReader(location string, blockSize int, bm block_manager.BlockManager) *FileReader {
	return &FileReader{
		block_manager:   bm,
		location:        location,
		currentBlock:    make([]byte, 0, blockSize),
		currentBlockNum: 0,
		blockSize:       blockSize,
		offsetInBlock:   0,
		allDataRead:     make([]byte, 0),
		direction:       true, // default to forward reading
	}
}

//direction lets us support reading from the end of the file or from the beginning

func (fr *FileReader) ReadEntry(blockNum int) ([]byte, error) {
	if len(fr.currentBlock) == 0 || fr.offsetInBlock >= len(fr.currentBlock) {
		block, err := fr.block_manager.ReadBlock(fr.location, blockNum, fr.direction)
		if err != nil {
			return nil, err
		}
		fr.currentBlock = block
		fr.offsetInBlock = 0
		fr.currentBlockNum = blockNum
	}

	if fr.offsetInBlock < len(fr.currentBlock) {
		entry := fr.currentBlock[fr.offsetInBlock : fr.offsetInBlock+fr.blockSize]
		fr.allDataRead = append(fr.allDataRead, entry...)
		fr.offsetInBlock += fr.blockSize
		return entry, nil
	}

	return nil, nil // No more data to read
}

func (fr *FileReader) SetDirection(forward bool) {
	fr.direction = forward
}
func (fr *FileReader) GetAllDataRead() []byte {
	return fr.allDataRead
}
