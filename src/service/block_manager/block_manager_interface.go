package block_manager

type BlockManagerInterface interface {
	WriteBlock(location string, blockNumber int, data []byte) error
	ReadBlock(location string, blockNumber int) ([]byte, error)
}
