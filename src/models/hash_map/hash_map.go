package hash_map

import (
	"nosqlEngine/src/models/key_value"
)

type HashMap struct {
	data map[string]string
	size int
}

// GetSize implements memtable.Memtable.
func (hmap *HashMap) GetSize() int {
	return hmap.size
}

func NewHashMap() *HashMap {
	return &HashMap{data: make(map[string]string), size: 0}
}

func (hmap *HashMap) Add(key string, value string) bool {
	hmap.data[key] = value
	hmap.size += (len(key) + len(value))
	return true
}
func (hmap *HashMap) Get(key string) (string, bool) {
	value, ok := hmap.data[key]
	return value, ok
}
func (hmap *HashMap) Remove(key string) bool {
	delete(hmap.data, key)
	return true
}
func (hmap *HashMap) ToRaw() []key_value.KeyValue {

	ret := make([]key_value.KeyValue, 0, len(hmap.data))

	for k, v := range hmap.data {
		ret = append(ret, key_value.NewKeyValue(k, v))
	}
	return ret
}

func (hmap *HashMap) Clear() bool {
	hmap.data = make(map[string]string)
	hmap.size = 0
	return true
}