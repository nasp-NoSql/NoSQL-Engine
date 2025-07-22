package engine

import (
	"nosqlEngine/src/config"
	"nosqlEngine/src/service/block_manager"
	"nosqlEngine/src/service/file_writer"
	"nosqlEngine/src/service/ss_compacter"
	"nosqlEngine/src/service/ss_parser"
	"nosqlEngine/src/service/user_limiter"
	"nosqlEngine/src/storage/memtable"
	"nosqlEngine/src/storage/wal"
)

var CONFIG = config.GetConfig()

type Engine struct {
	userLimiter  *user_limiter.UserLimiter
	memtables    []memtable.Memtable
	curr_mem_index int
	wal          wal.WAL
	ss_parser    ss_parser.SSParser
	ss_compacter  *ss_compacter.SSCompacterST
}

func NewEngine() *Engine {
	bm := block_manager.NewBlockManager()
	memtableCount := CONFIG.MemtableCount
	memtables := make([]memtable.Memtable, memtableCount)
	for i := 0; i < memtableCount; i++ {
		memtables[i] = memtable.NewMemtable()
	}
	return &Engine{
		userLimiter: user_limiter.NewUserLimiter(),
		memtables:   memtables,
		ss_parser: ss_parser.NewSSParser(file_writer.NewFileWriter(bm, CONFIG.BlockSize)),
		ss_compacter: ss_compacter.NewSSCompacterST(),
		wal:        wal.NewWAL("nosqlEngine/data/wal", 1, ),
	}
}
func (engine *Engine) SetNextMemtable() {
	engine.curr_mem_index = (engine.curr_mem_index + 1) % CONFIG.MemtableCount
}
func (engine *Engine) checkIfMemtableFull() bool {
	return engine.memtables[engine.curr_mem_index].GetSize() >= CONFIG.MemtableSize
}

func (engine *Engine) Start() {
// wal.Replay("holder")
}
func (engine *Engine) Close() error {
	return nil
}