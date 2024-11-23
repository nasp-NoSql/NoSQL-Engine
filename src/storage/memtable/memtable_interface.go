package memtable

type Memtable interface {
	Add(key string, value string) bool
	Get(key string) (string, bool)
	Remove(key string) bool
	ToRaw() ([]string, []string) // keys, values
}
