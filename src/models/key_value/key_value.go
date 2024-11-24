package key_value

import "sort"

type KeyValue struct {
	key   string
	value string
}

func NewKeyValue(key string, value string) KeyValue {
	return KeyValue{key: key, value: value}
}
func (kv KeyValue) GetKey() string {
	return kv.key
}
func (kv KeyValue) GetValue() string {
	return kv.value
}
func GetKeys(data []KeyValue) []string {
	keys := make([]string, 0, len(data))
	for i := 0; i < len(data); i++ {
		keys = append(keys, data[i].key)
	}
	return keys
}
func GetValues(data []KeyValue) []string {
	values := make([]string, 0, len(data))
	for i := 0; i < len(data); i++ {
		values = append(values, data[i].value)
	}
	return values
}
func SortByKeys(data *[]KeyValue) {
	sort.Slice(data, func(i, j int) bool {
		return (*data)[i].GetKey() < (*data)[j].GetKey()
	})
}
