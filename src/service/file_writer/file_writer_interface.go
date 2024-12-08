package file_writer

type FileWriter interface {
	WriteSS(data ...[]byte) bool
}
