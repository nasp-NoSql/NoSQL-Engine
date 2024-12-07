package service

import (
	"nosqlEngine/src/service/block_manager"
)

type Reader struct {
	block_manager block_manager.BlockManager
}

func NewReader(bm block_manager.BlockManager) *Reader {
	return &Reader{block_manager: bm}
}

func (r *Reader) ReadSS(path string, i int) ([]byte, error) {

	data, err := r.block_manager.ReadBlock(i, path)

	return data, err

}
