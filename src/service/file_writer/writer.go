package file_writer

import (
	"fmt"
	"nosqlEngine/src/service/block_manager"
	"path/filepath"

	"github.com/google/uuid"
)

type FileWriter struct {
	block_manager   block_manager.BlockManager
	location        string
	currentBlock    []byte
	currentBlockNum int
	blockSize       int
	offsetInBlock   int
	allDataWritten  []byte
}

func NewFileWriter(bm block_manager.BlockManager, blockSize int) *FileWriter {
	uuidStr := uuid.New().String()
	location := filepath.ToSlash(filepath.Join("../../../data", "sstable_"+uuidStr+".db"))
	return &FileWriter{
		block_manager:   bm,
		location:        location,
		currentBlock:    make([]byte, 0, blockSize),
		currentBlockNum: 0,
		blockSize:       blockSize,
		offsetInBlock:   0,
		allDataWritten:  make([]byte, 0),
	}
}

func (fw *FileWriter) WriteEntry(data []byte, sectionEnd bool) {

	if sectionEnd {
		if len(fw.currentBlock) > 0 {
			fw.FlushCurrentBlock()
		}
	}

	fmt.Print("Writing entry to FileWriter: ", data, "\n")
	if fw.IsJumbo(len(data)) {
		fw.WriteJumboData(data)
		return
	}

	if !fw.CanWrite(len(data)) {
		// Write current block to disk and start a new block
		fw.FlushCurrentBlock()
	}
	fw.currentBlock = append(fw.currentBlock, data...)
	fw.offsetInBlock += len(data)
}

// IsJumbo returns true if the data is larger than a single block
func (fw *FileWriter) IsJumbo(dataLen int) bool {
	return dataLen > fw.blockSize
}

// WriteJumboData splits and writes data that is larger than a block
func (fw *FileWriter) WriteJumboData(data []byte) {
	if len(fw.currentBlock) > 0 {
		fw.FlushCurrentBlock()
	}
	numBlocks := (len(data) + fw.blockSize - 1) / fw.blockSize // Calculate number of blocks needed
	for i := 0; i < numBlocks; i++ {
		start := i * fw.blockSize
		end := start + fw.blockSize
		if end > len(data) {
			end = len(data)
		}
		wrData := data[start:end]
		//ensure it is padded to block size
		if len(wrData) < fw.blockSize {
			padding := make([]byte, fw.blockSize-len(wrData))
			wrData = append(wrData, padding...)
		}
		fw.allDataWritten = append(fw.allDataWritten, wrData...)
		err := fw.block_manager.WriteBlock(fw.location, fw.currentBlockNum, wrData)
		if err != nil {
			return
		}
		fw.currentBlockNum++
		fw.currentBlock = make([]byte, 0, fw.blockSize) // Reset current block
		fw.offsetInBlock = 0
	}
}

// CanWrite checks if the data can fit in the current block
func (fw *FileWriter) CanWrite(dataLen int) bool {
	return fw.offsetInBlock+dataLen <= fw.blockSize
}

// FlushCurrentBlock writes the current block to disk and starts a new block
func (fw *FileWriter) FlushCurrentBlock() {
	if len(fw.currentBlock) > 0 {
		//add padding to ensure block size
		if len(fw.currentBlock) < fw.blockSize {
			padding := make([]byte, fw.blockSize-len(fw.currentBlock))
			fw.currentBlock = append(fw.currentBlock, padding...)
		}
		fw.allDataWritten = append(fw.allDataWritten, fw.currentBlock...)
		fw.block_manager.WriteBlock(fw.location, fw.currentBlockNum, fw.currentBlock)
		fw.currentBlockNum++
		fw.currentBlock = make([]byte, 0, fw.blockSize)
		fw.offsetInBlock = 0
	}
}

func (fw *FileWriter) GetAllDataWritten() []byte {
	return fw.allDataWritten
}

func (fw *FileWriter) GetLocation() string {
	return fw.location
}
