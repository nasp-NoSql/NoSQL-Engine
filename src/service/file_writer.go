package service

type FileWriterImpl struct {
}

func NewFileWriter() *FileWriterImpl {
	return &FileWriterImpl{}
}
func (fw *FileWriterImpl) WriteSS(data []byte) bool {
	return true
}
