package ss_compacter

import "nosqlEngine/src/service/retriever"

func updateValsAndCounts(keys []string, vals []string, counts []int, pool *retriever.EntryRetrieverPool) {
	for i := 0; i < len(vals); i++ {
		if counts[i] == 0 {
			keys[i] = ""
			continue
		}
		if keys[i] == "" {
			keys[i], vals[i], _, _ = pool.ReadNextVal(i) // Read the
			counts[i]--
		}
	}
}
func getMinValIndex(vals []string) int {
	minVal := vals[0]
	minIdx := 0
	for i, val := range vals {
		if val == "" {
			continue
		}
		if val < minVal {
			minVal = val
			minIdx = i
		}
	}
	return minIdx
}
func removeDuplicateKeys(keys []string, fromIndex int, pool *retriever.EntryRetrieverPool) {
	for i := fromIndex + 1; i < len(keys); i++ {
		if keys[fromIndex] == keys[i] {
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
