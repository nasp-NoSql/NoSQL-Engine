package service

type Reader struct{}

func NewReader() *Reader {
	return &Reader{}
}

func (r *Reader) ReadBlock(size int) ([]byte, error) {
	return nil, nil
}
