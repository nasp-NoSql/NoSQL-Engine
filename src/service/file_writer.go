package service

type FileWriter struct {
}

func NewFileWriter() *FileWriter {
	return &FileWriter{}
}
func (fw *FileWriter) WriteSS(data []byte) bool {
	return true
}
