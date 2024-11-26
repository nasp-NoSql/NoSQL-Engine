package service

type FileReader interface {
	ReadBlock(size int) ([]byte, error)
}
