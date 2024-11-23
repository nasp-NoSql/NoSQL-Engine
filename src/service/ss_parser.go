package service

import "sort"

type KeyValue struct {
	key   string
	value string
}

type SSParser struct {
	mems      [][]KeyValue
	isParsing bool
}

func NewSSParser() *SSParser {
	return &SSParser{mems: make([][]KeyValue, 0), isParsing: false}
}

func (ssParser *SSParser) AddMemtable(keys []string, values []string) {

	keyValue := make([]KeyValue, 0, len(keys))

	for i := 0; i < len(keys); i++ {
		keyValue = append(keyValue, KeyValue{key: keys[i], value: values[i]})
	}
	ssParser.mems = append(ssParser.mems, keyValue)
	ssParser.parseMemtable()
}

func (ssParser *SSParser) parseMemtable() {
	if ssParser.isParsing {
		return
	}
	ssParser.isParsing = true

	data := ssParser.mems[0]
	ssParser.mems = ssParser.mems[1:]
	sortKeyValueData(&data)

	//bloomBytes = bloom_filter.GetBloomFilter(keys)
	//merkleTree = merkle_tree.GetMerkleTree(keys, values)

	//file_writer.WriteSS(bytes)

	if len(ssParser.mems) != 0 {
		ssParser.parseMemtable()
	} else {
		ssParser.isParsing = false
	}

}

func sortKeyValueData(data *[]KeyValue) {
	// sort by key
	sort.Slice(*data, func(i, j int) bool {
		return (*data)[i].key < (*data)[j].key
	})
}

func serializeDataGetOffsets(data []string) ([]byte, []int64) {
	var size int64 = 0
	for i := 0; i < len(data); i++ {
		size += int64(len(data[i]))
	}
	binary := make([]byte, 0, size)
	offsets := make([]int64, 0, len(data))

	for i := 0; i < len(data); i++ {
		binValue := []byte(data[i])
		binary = append(binary, binValue...)
		offsets = append(offsets, int64(len(binary)))
	}
	return binary, offsets
}
