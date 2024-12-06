package utils

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
)

// getPaths returns the paths of all the files in the sstable folder

func GetPaths() []string {

	folderPath := "../../data/sstable"
	ret := make([]string, 0)
	files, err := os.ReadDir(folderPath)
	if err != nil {
		log.Fatalf("Failed to read directory: %v", err)
	}

	for _, file := range files {
		fmt.Println("File Name:", file.Name())
		path := filepath.Join(folderPath, file.Name())

		if file.IsDir() {
			fmt.Println("This is a directory.")
		} else {
			fmt.Println("This is a file.")
			ret = append(ret, path)
		}
	}

	return ret
}
