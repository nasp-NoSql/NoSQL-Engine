package ss_compacter

import (
	"nosqlEngine/src/service/retriever"
)

func updateValsAndCounts(keys []string, vals []string, counts []int, pool *retriever.EntryRetrieverPool) {
	for i := 0; i < len(vals); i++ {
		if counts[i] == 0 {
			keys[i] = ""
			vals[i] = ""
			continue
		}
		if keys[i] == "" {
			counts[i]--
			if counts[i] != 0 {
				keys[i], vals[i], _, _ = pool.ReadNextVal(i) // Read the next value
			}
		}
	}
}
func getMinValIndex(keys []string, values []string) int {
	minVal := "\xFF\xFF\xFF\xFF" // Maximum possible string value
	minIdx := -1
	for i, key := range keys {
		if key == "" {
			continue
		}
		if key < minVal {
			minVal = key
			minIdx = i
		}
	}
	// Check if TOMBSTONE for same key exists
	for i := 0; i < len(keys); i++ {
		if keys[i] == keys[minIdx] && values[i] == CONFIG.Tombstone {
			return i
		}
	}
	return minIdx
}
func removeDuplicateKeys(keys []string, index int) {
	for i := 0; i < len(keys); i++ {
		if i == index || keys[i] == "" {
			continue
		}
		if keys[index] == keys[i] {
			print("Removing duplicate key: ", keys[i], " at index ", i, "\n")
			keys[i] = ""
		}
	}
}

func areAllValuesZero(values []int) bool {
	for _, value := range values {
		if value != 0 {
			return false
		}
	}
	return true
}
