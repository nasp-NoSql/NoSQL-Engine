package file_reader

type FileReader interface {
	ReadSS(path string, i int) ([]byte, error)
}
