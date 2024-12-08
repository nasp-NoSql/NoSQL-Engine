package file_writer

type FileWriterMFile struct {
}

func NewFileWriterMFile() *FileWriterMFile {
	return &FileWriterMFile{}
}
func (fw *FileWriterMFile) WriteSS(data ...[]byte) bool {
	dataBytes := data[0]
	indexBytes := data[1]
	summaryBytes := data[2]
	metaDataBytes := data[3]
	_ = dataBytes // just not to have err
	_ = indexBytes // just not to have err
	_ = summaryBytes // just not to have err
	_ = metaDataBytes // just not to have err
	return true
}
