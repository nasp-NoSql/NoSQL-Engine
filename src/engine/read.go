package engine

import (
	"fmt"
	rw "nosqlEngine/src/service/file_reader"
	r "nosqlEngine/src/service"

)

func (engine *Engine) Read(user string, key string) (string, error) {
	// Read from memtables
	if ok, err := engine.userLimiter.CheckUserTokens(user); !ok {
		return "", fmt.Errorf("user %s is not allowed to read: %w", user, err)
	}
	for _, mem := range engine.memtables {
		if value, ok := mem.Get(key); ok {
			// Found in memtable, return value
			return value, nil
		}
	}
	// Not found in memtables, read from SSTables
	reader := rw.NewFileReader('', CONFIG.BlockSize, *engine.block_manager) // need to get random name
	retriever := r.NewEntryRetriever(*reader)

	return retriever.RetrieveEntry(key)
}
