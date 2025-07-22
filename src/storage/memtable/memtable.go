package memtable

import (
	"nosqlEngine/src/config"
	"nosqlEngine/src/models/hash_map"
)

var CONFIG = config.GetConfig()

func NewMemtable() Memtable {
	var memtable Memtable
	switch CONFIG.MemtableType {
	case "hashmap":
		memtable = hash_map.NewHashMap()
	case "skiplist":
		memtable = hash_map.NewHashMap() //skiplist
	default:
		memtable = hash_map.NewHashMap() // b-tree
	}
	return memtable
}