package hashmap

type HashMap struct {
	data map[string]string
	size int64
}

func NewHashMap() *HashMap {
	return &HashMap{data: make(map[string]string), size: 0}
}

func (hmap *HashMap) Add(key string, value string) bool {
	hmap.data[key] = value
	hmap.size += (int64)(len(key) + len(value))
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
func (hmap *HashMap) ToRaw() ([]string, []string) {
	keys := make([]string, 0, len(hmap.data))
	values := make([]string, 0, len(hmap.data))

	for k, v := range hmap.data {
		keys = append(keys, k)
		values = append(values, v)
	}
	return keys, values
}
