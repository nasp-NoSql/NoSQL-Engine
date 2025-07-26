package ss_compacter

func updateValsAndCounts(vals [][]byte, counts []int, readers []*FileReader) {
	for i := 0; i < len(vals); i++ {
		if counts[i] == 0 {
			vals[i] = nil
			continue
		}
		if vals[i] == nil {
			vals[i] = readers[i].ReadNextVal() // gets next entry (key size, key) || (<value size, value>)f
			counts[i]--
		}
	}
}
func getMinValIndex(vals [][]byte) int {
	minVal := make([]byte, 0)
	minIdx := -1
	for i, val := range vals {
		if val == nil {
			continue
		}
		if len(minVal) == 0 || string(val) < string(minVal) {
			minVal = val
			minIdx = i
		}
	}
	return minIdx
}
func removeDuplicateKeys(keys [][]byte, fromIndex int, readers []*FileReader) {
	for i := fromIndex + 1; i < len(readers); i++ {
		if string(keys[fromIndex]) == string(keys[i]) {
			keys[i] = nil
			readers[i].ReadNextVal() // Read next value to remove the value
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
