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

func (fw *FileWriter) Write(data []byte, sectionEnd bool, size []byte) int {
	if sectionEnd {
		if size != nil && len(size) > 0 {
			fw.FlushWithSize(size)
		}
		if len(fw.currentBlock) > 0 {
			fw.FlushCurrentBlock()
		}
	}

	if fw.IsJumbo(len(data)) {
		fmt.Println("This is a jumbo entry, allocating multiple blocks!")
		fw.WriteJumboData(data)
		return fw.currentBlockNum
	}

	if !fw.CanWrite(len(data)) {
		// Write current block to disk and start a new block
		fw.FlushCurrentBlock()
	}
	fw.currentBlock = append(fw.currentBlock, data...)
	fw.offsetInBlock += len(data)

	return fw.currentBlockNum
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
	numBlocks := (len(data) + 8 + fw.blockSize - 1) / fw.blockSize // Calculate number of blocks needed
	for i := 0; i < numBlocks; i++ {
		start := i * fw.blockSize
		end := start + fw.blockSize - 8
		if end > len(data)-8 {
			end = len(data) - 8
		}
		wrData := data[start:end]

		// Add jumbo flag (1 = jumbo block)
		jumboFlag := make([]byte, 8)
		jumboFlag[7] = 1 // Set last byte to 1 to indicate jumbo block
		if len(wrData)+8 < fw.blockSize {
			padding := make([]byte, fw.blockSize-len(wrData)-8) //
			wrData = append(wrData, padding...)
		}
		// Add jumbo flag at the end (no padding needed for jumbo blocks)
		wrData = append(wrData, jumboFlag...)

		fmt.Printf("Writing jumbo block %d with size %d bytes\n", fw.currentBlockNum, len(wrData))
		fw.allDataWritten = append(fw.allDataWritten, wrData...)
		err := fw.block_manager.WriteBlock(fw.location, fw.currentBlockNum, wrData)

		if err != nil {
			fmt.Printf("Error writing jumbo block %d: %v\n", fw.currentBlockNum, err)
			return
		}
		fw.currentBlockNum++
		fw.currentBlock = make([]byte, 0, fw.blockSize) // Reset current block
		fw.offsetInBlock = 0
	}
}

// CanWrite checks if the data can fit in the current block (reserving 8 bytes for jumbo flag)
func (fw *FileWriter) CanWrite(dataLen int) bool {
	return fw.offsetInBlock+dataLen+8 <= fw.blockSize // Reserve 8 bytes for jumbo flag
}

// FlushCurrentBlock writes the current block to disk and starts a new block
func (fw *FileWriter) FlushCurrentBlock() {
	// when flushing we add a flag at the end of data to indicate that the rest is padding
	if len(fw.currentBlock) > 0 {
		// Add jumbo flag (0 = not jumbo, 1 = jumbo)
		jumboFlag := make([]byte, 8)
		//notation := "<!>" //data end notation
		// Set flag to 0 (not jumbo) - binary.BigEndian.PutUint64(jumboFlag, 0) sets all bytes to 0

		//add padding to ensure block size (accounting for 8-byte jumbo flag)
		if len(fw.currentBlock)+8 < fw.blockSize {
			padding := make([]byte, fw.blockSize-len(fw.currentBlock)-8)
			//fw.currentBlock = append(fw.currentBlock, notation...)
			fw.currentBlock = append(fw.currentBlock, padding...)
		}

		// Add jumbo flag at the end
		fw.currentBlock = append(fw.currentBlock, jumboFlag...)

		fmt.Printf("Flushing block %d with size %d bytes\n", fw.currentBlockNum, len(fw.currentBlock))
		fmt.Printf("Current block data: %v\n", fw.currentBlock)
		fw.allDataWritten = append(fw.allDataWritten, fw.currentBlock...)
		fw.block_manager.WriteBlock(fw.location, fw.currentBlockNum, fw.currentBlock)
		fw.currentBlockNum++
		fw.currentBlock = make([]byte, 0, fw.blockSize)
		fw.offsetInBlock = 0
	}
}

func (fw *FileWriter) FlushWithSize(size []byte) {
	//this is the same flush bit instead of padding to the top, at the last 8bytes there is size var
	fmt.Printf("FLUSH WITH SIZE: %v\n", size)

	// Add jumbo flag (0 = not jumbo)
	jumboFlag := make([]byte, 8)

	padding := make([]byte, fw.blockSize-len(fw.currentBlock)-8-8) // 8 bytes for size, 8 bytes for jumbo flag
	fw.currentBlock = append(fw.currentBlock, padding...)
	fw.currentBlock = append(fw.currentBlock, size...)
	fw.currentBlock = append(fw.currentBlock, jumboFlag...) // Add jumbo flag at the very end

	fw.allDataWritten = append(fw.allDataWritten, fw.currentBlock...)
	fw.block_manager.WriteBlock(fw.location, fw.currentBlockNum, fw.currentBlock)
	fw.currentBlockNum++
	fw.currentBlock = make([]byte, 0, fw.blockSize)
	fw.offsetInBlock = 0
}
func (fw *FileWriter) GetAllDataWritten() []byte {
	return fw.allDataWritten
}

func (fw *FileWriter) GetLocation() string {
	return fw.location
}

func (fw *FileWriter) GetCurrentBlockNum() int {
	return fw.currentBlockNum
}
