package tests

import (
	"nosqlEngine/src/models/key_value"
	"testing"
)

type FileReaderMock struct {
	rawBytes []byte
}

func TestRetrieveAKey(t *testing.T) {

	dataBytes, dataOffsets := serializeDataGetOffsets(key_value.GetValues(data))
	indexBytes, indexOffsets := serializeIndexGetOffsets(key_value.GetKeys(data), dataOffsets, int64(len(dataBytes)))
	summaryBytes := getSummaryBytes(key_value.GetKeys(data), indexOffsets)
	metaDataBytes := getMetaDataBytes(int64(len(dataBytes)+len(indexBytes)), int64(len(summaryBytes)), make([]byte, 0), make([]byte, 0))

	bytes := make([]byte, 0, len(dataBytes)+len(indexBytes)+len(summaryBytes))
	bytes = append(bytes, dataBytes...)
	bytes = append(bytes, indexBytes...)
	bytes = append(bytes, summaryBytes...)
	bytes = append(bytes, metaDataBytes...)

}
