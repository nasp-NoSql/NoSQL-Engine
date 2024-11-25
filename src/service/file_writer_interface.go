package service

type FileWriter interface {
	WriteSS(data []byte) bool
}
