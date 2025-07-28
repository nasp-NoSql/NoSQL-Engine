package memtable

import "nosqlEngine/src/models/key_value"

type Memtable interface {
	Add(key string, value string) bool
	Get(key string) (string, bool)
	ToRaw() []key_value.KeyValue // keys, values
	GetSize() int
	Clear() bool
}

