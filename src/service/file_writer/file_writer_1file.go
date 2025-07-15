package file_writer

import (
	"fmt"
	"nosqlEngine/src/service/block_manager"

	"github.com/google/uuid"
)
type FileWriter struct{
	location string
} // Mock for file writer, can be replaced with actual implementation
func NewFileWriter(location string) *FileWriter {
	return &FileWriter{location: location}
}
func (fw *FileWriter) Write(data []byte, endSection bool) int {
	return 2 // index of block it writes it in
}
func (fw *FileWriter) Close()  bool{
	return true
}
func (fw *FileWriter) NewFile(location string) {
	fw.Close() // Close the current file before creating a new one
	fw.location = location
}



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
