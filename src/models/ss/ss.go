package ss

type KeyValue struct {
	Key   string
	Value string
}

type SStable struct {
	Data  []KeyValue
	Index map[string]int
}

func createNewSSTable() *SStable {
	return &SStable{
		Data:  make([]KeyValue, 0),
		Index: make(map[string]int),
	}
}

func (s *SStable) addKeyValue(key string, value string) {
	s.Data = append(s.Data, KeyValue{Key: key, Value: value})
}
