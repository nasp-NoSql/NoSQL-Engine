package service

type FileWriter1File struct {
}

func NewFileWriter1File() *FileWriter1File {
	return &FileWriter1File{}
}
func (fw *FileWriter1File) WriteSS(data ...[]byte) bool {
	fullRaw := data[0]
	_ = fullRaw // just not to have err
	return true
}



