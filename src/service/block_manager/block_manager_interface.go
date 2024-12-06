package block_manager

type BlockManager interface {
	WriteBlocks(data []byte, filename string) bool

	ReadBlock(blockId int, filename string) ([]byte, error)
}
