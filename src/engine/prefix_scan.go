package engine

import "fmt"

func (engine *Engine) PrefixScan(user string, prefix string) ([]string, error) {
	// Check if user is allowed to read
	if ok, err := engine.userLimiter.CheckUserTokens(user); !ok {
		return nil, fmt.Errorf("user %s is not allowed to read: %w", user, err)
	}

	results := make([]string, 0)

	// Scan through memtables
	for _, mem := range engine.memtables {
		for _, kv := range mem.ToRaw() {
			if len(kv.GetKey()) >= len(prefix) && kv.GetKey()[:len(prefix)] == prefix {
				results = append(results, kv.GetValue())
			}
		}
	}

	// If not found in memtables, read from SSTables

	return results, nil
}
