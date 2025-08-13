package engine

import (
	"fmt"
	"nosqlEngine/src/service/retriever"
)

type RangeIterator struct {
	data    [][]string
	index   int
	stopped bool
}

func NewRangeIterator(results map[string]string) *RangeIterator {
	iterator_data := SortKeysAndVals(results)
	return &RangeIterator{data: iterator_data, index: 0, stopped: false}
}

func (ri *RangeIterator) Next() (string, string, bool) {
	if ri.stopped || ri.index >= len(ri.data) {
		return "", "", false
	}

	key := ri.data[ri.index][0]
	value := ri.data[ri.index][1]
	ri.index++

	hasNext := ri.index < len(ri.data)
	return key, value, hasNext
}

func (ri *RangeIterator) Stop() {
	ri.stopped = true
}

func (ri *RangeIterator) Reset() {
	ri.index = 0
	ri.stopped = false
}

func (ri *RangeIterator) HasNext() bool {
	return !ri.stopped && ri.index < len(ri.data)
}

func (engine *Engine) RangeIterate(user string, start string, end string) (*RangeIterator, error) {
	if ok, err := engine.userLimiter.CheckUserTokens(user); !ok {
		return nil, fmt.Errorf("user %s is not allowed to read: %w", user, err)
	}
	results, err := engine.findAllRangeMatches(start, end)
	if err != nil {
		return nil, fmt.Errorf("failed to find range matches: %w", err)
	}
	return NewRangeIterator(results), nil
}

func (engine *Engine) RangeScan(user string, start string, end string, pageNum int, pageSize int) [][]string {
	results, _ := engine.findAllRangeMatches(start, end)

	sorted := SortKeysAndVals(results)
	return sorted[min(len(sorted), (pageNum-1)*pageSize):min(len(sorted), pageNum*pageSize)]
}

func (engine *Engine) findAllRangeMatches(start string, end string) (map[string]string, error) {
	results := make(map[string]string)

	// Scan through memtables
	for _, mem := range engine.memtables {
		for _, kv := range mem.ToRaw() {
			fmt.Println(kv.GetKey(), kv.GetValue())
			if kv.GetKey() >= start && kv.GetKey() <= end {
				results[kv.GetKey()] = kv.GetValue()
			}
		}
	}

	// If not found in memtables, read from SSTables

	mretriever := retriever.NewMultiRetriever(engine.block_manager)

	retriever_results, err := mretriever.GetRangeEntries(start, end)
	fmt.Print(retriever_results, results)

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