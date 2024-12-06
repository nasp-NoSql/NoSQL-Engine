package service

type FileReader interface {
	ReadSS(path string, i int) ([]byte, error)
}
