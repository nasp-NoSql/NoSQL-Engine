package engine

// import (
// 	"fmt"
// 	"nosqlEngine/src/service/retriever"
// )

// func (engine *Engine) RangeScan(user string, start string, end string, pageNum int, pageSize int) {
// 	results, _ := engine.findAllRangeMatches(start, end)

// 	SortKeysAndVals(results)
// }

// func (engine *Engine) findAllRangeMatches(start string, end string) (map[string]string, error) {
// 	results := make(map[string]string)

// 	// Scan through memtables
// 	for _, mem := range engine.memtables {
// 		for _, kv := range mem.ToRaw() {
// 			if kv.GetKey() > start && kv.GetKey() < end {
// 				results[kv.GetKey()] = kv.GetValue()
// 			}
// 		}
// 	}

// 	// If not found in memtables, read from SSTables

// 	mretriever := retriever.NewMultiRetriever(engine.block_manager)

// 	retriever_results, err := mretriever.GetRangeEntries(start, end)

// 	if err != nil {
// 		fmt.Print("Failed to retrieve results from SSTables")
// 	}

// 	for key, value := range retriever_results {
// 		if _, exists := results[key]; !exists {
// 			results[key] = value
// 		}
// 	}
// 	return results, nil
// }
