package engine

import (
	"fmt"
	"nosqlEngine/src/service/retriever"
	"sort"
)

type PrefixIterator struct {
	data    [][]string
	index   int
	stopped bool
}

func NewPrefixIterator(results map[string]string) *PrefixIterator {
	iterator_data := SortKeysAndVals(results)
	return &PrefixIterator{data: iterator_data, index: 0, stopped: false}
}

func (pi *PrefixIterator) Next() (string, string, bool) {
	if pi.stopped || pi.index >= len(pi.data) {
		return "", "", false
	}

	key := pi.data[pi.index][0]
	value := pi.data[pi.index][1]
	pi.index++

	hasNext := pi.index < len(pi.data)
	return key, value, hasNext
}

func (pi *PrefixIterator) Stop() {
	pi.stopped = true
}

func (pi *PrefixIterator) Reset() {
	pi.index = 0
	pi.stopped = false
}

func (pi *PrefixIterator) HasNext() bool {
	return !pi.stopped && pi.index < len(pi.data)
}

func (engine *Engine) PrefixIterate(user string, prefix string) (*PrefixIterator, error) {
	if ok, err := engine.userLimiter.CheckUserTokens(user); !ok {
		return nil, fmt.Errorf("user %s is not allowed to read: %w", user, err)
	}
	results, err := engine.findAllPrefixMatches(prefix)
	if err != nil {
		return nil, fmt.Errorf("failed to find prefix matches: %w", err)
	}
	return NewPrefixIterator(results), nil
}

func SortKeysAndVals(data map[string]string) [][]string {
	result_array := make([][]string, 0)
	for key, value := range data {
		tmp := []string{key, value}
		result_array = append(result_array, tmp)
	}

	sort.Slice(result_array, func(i, j int) bool {
		if len(result_array[i]) <= 1 || len(result_array[j]) <= 1 {
			return len(result_array[i]) < len(result_array[j])
		}
		return result_array[i][0] < result_array[j][0]
	})
	return result_array
}

func (engine *Engine) PrefixScan(user string, prefix string, pageNum int, pageSize int) {
	results, _ := engine.findAllPrefixMatches(prefix)

	SortKeysAndVals(results)
}

func (engine *Engine) findAllPrefixMatches(prefix string) (map[string]string, error) {
	results := make(map[string]string)

	// Scan through memtables
	for _, mem := range engine.memtables {
		for _, kv := range mem.ToRaw() {
			if len(kv.GetKey()) >= len(prefix) && kv.GetKey()[:len(prefix)] == prefix {
				results[kv.GetKey()] = kv.GetValue()
			}
		}
	}

	// If not found in memtables, read from SSTables

	mretriever := retriever.NewMultiRetriever(engine.block_manager)

	retriever_results, err := mretriever.GetPrefixEntries(prefix)

	if err != nil {
		fmt.Print("Failed to retrieve results from SSTables")
	}

	for key, value := range retriever_results {
		if _, exists := results[key]; !exists {
			results[key] = value
		}
	}
	return results, nil
}
