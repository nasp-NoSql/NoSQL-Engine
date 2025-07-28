package utils

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
)

// getPaths returns the paths of all the files in the sstable folder
func getProjectRoot() string {
	_, filename, _, _ := runtime.Caller(0)
	// Go up from src/service/file_writer/writer.go to project root
	projectRoot := filepath.Dir(filepath.Dir(filepath.Dir(filename)))
	return projectRoot
}

func GetPaths(relativePath string, ext string) []string {
	folderPath := filepath.Join(getProjectRoot(), relativePath)
	var paths []string
	files, _ := os.ReadDir(folderPath)
	for _, file := range files {
		if !strings.HasSuffix(file.Name(), ext) {
			continue
		}
		paths = append(paths, filepath.Join(folderPath, file.Name()))
	}
	return paths
}
