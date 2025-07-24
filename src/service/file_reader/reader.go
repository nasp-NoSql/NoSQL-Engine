package file_reader

import (
	"bytes"
	"fmt"
	"nosqlEngine/src/service/block_manager"
)

// Jumbo flag constants - matching the writer
const (
	JumboStart  = 1 // 00000001 - First block in jumbo sequence
	JumboMiddle = 3 // 00000011 - Middle block in jumbo sequence
	JumboEnd    = 7 // 00000111 - Last block in jumbo sequence
	NonJumbo    = 0 // 00000000 - Regular non-jumbo block
)

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

// Read reads an entry from the specified block, handling jumbo blocks and data cleaning
func (fr *FileReader) Read(blockNum int) ([]byte, int, error) {
	// Read the block
	block, err := fr.block_manager.ReadBlock(fr.location, blockNum, fr.direction)
	if err != nil {
		return nil, 0, err
	}

	// Extract jumbo flag (last 3 bytes)
	if len(block) < 3 {
		return nil, 0, fmt.Errorf("block too small: %d bytes", len(block))
	}

	jumboFlag := block[len(block)-1]
	fmt.Printf("Reading block %d, jumbo flag: %d\n", blockNum, jumboFlag)

	switch jumboFlag {
	case NonJumbo:
		// Single block entry
		return fr.cleanBlockData(block), 1, nil

	case JumboStart, JumboMiddle, JumboEnd:
		// Jumbo block - need to read the entire sequence
		return fr.readJumboSequence(blockNum, jumboFlag)

	default:
		return nil, 0, fmt.Errorf("unknown jumbo flag: %d", jumboFlag)
	}
}

// readJumboSequence reads all blocks in a jumbo sequence and returns the complete entry
func (fr *FileReader) readJumboSequence(startBlockNum int, initialFlag byte) ([]byte, int, error) {
	currentBlockNum := startBlockNum

	if fr.direction {
		// Forward reading: Start -> Middle -> End
		return fr.readJumboForward(currentBlockNum, initialFlag)
	} else {
		// Backward reading: End -> Middle -> Start
		return fr.readJumboBackward(currentBlockNum, initialFlag)
	}
}

// readJumboForward reads jumbo sequence in forward direction
func (fr *FileReader) readJumboForward(startBlockNum int, initialFlag byte) ([]byte, int, error) {
	var jumboData []byte
	currentBlockNum := startBlockNum

	// First block should be JumboStart
	if initialFlag != JumboStart {
		return nil, 0, fmt.Errorf("expected JumboStart flag, got %d", initialFlag)
	}
	readBlocks := 0
	for {
		block, err := fr.block_manager.ReadBlock(fr.location, currentBlockNum, fr.direction)
		if err != nil {
			return nil, 0, err
		}

		jumboFlag := block[len(block)-1]
		cleanData := fr.cleanBlockData(block)
		jumboData = append(jumboData, cleanData...)

		fmt.Printf("Read jumbo block %d with flag %d, data length: %d\n", currentBlockNum, jumboFlag, len(cleanData))

		if jumboFlag == JumboEnd {
			break // End of jumbo sequence
		}
		readBlocks++
		currentBlockNum--
	}

	return jumboData, readBlocks, nil
}

// readJumboBackward reads jumbo sequence in backward direction
func (fr *FileReader) readJumboBackward(startBlockNum int, initialFlag byte) ([]byte, int, error) {
	var jumboData []byte
	currentBlockNum := startBlockNum

	// First block should be JumboEnd when reading backward
	if initialFlag != JumboEnd {
		return nil, 0, fmt.Errorf("expected JumboEnd flag when reading backward, got %d", initialFlag)
	}

	// Collect blocks in reverse order
	var blocks [][]byte
	readBlocks := 0
	for {
		block, err := fr.block_manager.ReadBlock(fr.location, currentBlockNum, fr.direction)
		if err != nil {
			return nil, 0, err
		}

		jumboFlag := block[len(block)-1]
		cleanData := fr.cleanBlockData(block)
		blocks = append(blocks, cleanData)

		fmt.Printf("Read jumbo block %d with flag %d, data length: %d\n", currentBlockNum, jumboFlag, len(cleanData))

		if jumboFlag == JumboStart {
			break // End of jumbo sequence (start when reading backward)
		}
		readBlocks++
		currentBlockNum++
	}

	// Reverse the blocks to get correct order
	for i := len(blocks) - 1; i >= 0; i-- {
		jumboData = append(jumboData, blocks[i]...)
	}

	return jumboData, readBlocks, nil
}

// cleanBlockData removes the jumbo flag and padding, returning only the actual data
func (fr *FileReader) cleanBlockData(block []byte) []byte {
	if len(block) < 6 { // At least 3 bytes for notation + 3 bytes for jumbo flag
		return []byte{}
	}

	// Remove the last 3 bytes (jumbo flag)
	dataWithNotation := block[:len(block)-3]

	// Find the notation "<!>" to separate data from padding
	notation := []byte("<!>")
	notationIndex := bytes.Index(dataWithNotation, notation)

	if notationIndex == -1 {
		// No notation found, return all data (shouldn't happen in normal cases)
		fmt.Printf("Warning: No end notation found in block\n")
		return dataWithNotation
	}

	// Return only the data before the notation
	cleanData := dataWithNotation[:notationIndex]
	fmt.Printf("Cleaned block data: %d bytes (removed %d bytes of padding)\n",
		len(cleanData), len(dataWithNotation)-notationIndex)

	return cleanData
}

func (fr *FileReader) SetDirection(forward bool) {
	fr.direction = forward
}

func (fr *FileReader) GetAllDataRead() []byte {
	return fr.allDataRead
}

// GetJumboFlagName returns a human-readable name for the jumbo flag
func GetJumboFlagName(flag byte) string {
	switch flag {
	case JumboStart:
		return "JUMBO_START"
	case JumboMiddle:
		return "JUMBO_MIDDLE"
	case JumboEnd:
		return "JUMBO_END"
	case NonJumbo:
		return "NON_JUMBO"
	default:
		return "UNKNOWN"
	}
}

// ReadEntry reads a complete entry (handling both regular and jumbo blocks)
func (fr *FileReader) ReadEntry(blockNum int) ([]byte, int, error) {
	entry, readBlocks, err := fr.Read(blockNum)
	if err != nil {
		return nil, 0, err
	}

	if entry != nil {
		fr.allDataRead = append(fr.allDataRead, entry...)
	}

	return entry, readBlocks, nil
}

// get location of the file
func (fr *FileReader) GetLocation() string {
	return fr.location
}

// set location
func (fr *FileReader) SetLocation(location string) {
	fr.location = location
}
