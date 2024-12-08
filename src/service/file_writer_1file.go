package service

import (
	"fmt"
	"nosqlEngine/src/service/block_manager"

	"github.com/google/uuid"
)

type FileWriter1File struct {
	block_manager block_manager.BlockManager
}

func NewFileWriter1File(bm block_manager.BlockManager) *FileWriter1File {
	return &FileWriter1File{block_manager: bm}
}
func (fw *FileWriter1File) WriteSS(data ...[]byte) bool {
	fullRaw := data[0]
	_ = fullRaw
	fmt.Println("Data length: ", len(fullRaw))
	filename := fmt.Sprintf("../../data/sstable/sstable_%s.db", generateFileName())
	flag := fw.block_manager.WriteBlocks(fullRaw, filename)

	return flag
}

func generateFileName() string {
	return uuid.New().String()
}
