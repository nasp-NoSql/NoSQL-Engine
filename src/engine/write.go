package engine

import "fmt"


func (engine *Engine) Write(user string,key string, value string) error {

	// check if memory full
	if engine.checkIfMemtableFull() {
		return fmt.Errorf("memory is full, cannot write")
	}

	// Add to WAL
	if ok, err := engine.userLimiter.CheckUserTokens(user); !ok {
		return fmt.Errorf("user %s is not allowed to write: %w", user, err)
	}
	// write to WAL
	var ok error
	if value == CONFIG.Tombstone {
		ok = engine.wal.WriteDelete(key)
	} else {
		ok = engine.wal.WritePut(key, value)
	}
	if ok != nil {
		return fmt.Errorf("failed to write to WAL: %w", ok)
	}

	write_mem := engine.memtables[engine.curr_mem_index]
	// Add to memtable
	write_mem.Add(key, value)

	// move to diff goroutine and compact logic if needed
	if write_mem.GetSize() >= CONFIG.MemtableSize {
		engine.ss_parser.AddMemtable(write_mem.ToRaw())
		engine.SetNextMemtable() // Set to next memtable
	}
	return nil
}