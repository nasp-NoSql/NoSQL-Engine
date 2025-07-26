package ss_parser

import (
	"encoding/binary"
	"nosqlEngine/src/config"
	"nosqlEngine/src/models/key_value"
	"nosqlEngine/src/service/file_writer"
)

var CONFIG = config.GetConfig()

func SerializeDataGetOffsets(fw file_writer.FileWriterInterface, keyValues []key_value.KeyValue) ([]string, []int) {
	keys := make([]string, len(keyValues))
	offsets := make([]int, len(keyValues))
	for i := 0; i < len(keyValues); i++ {
		value := append(SizeAndValueToBytes(keyValues[i].GetKey()), SizeAndValueToBytes(keyValues[i].GetValue())...)
		blockIndex := fw.Write(value, false, nil)
		keys[i] = keyValues[i].GetKey()
		offsets[i] = blockIndex
	}
	return keys, offsets
}

func SerializeIndexGetOffsets(keys []string, offsets []int, fw file_writer.FileWriterInterface) ([]string, []int) {

	elNum := len(keys) / CONFIG.SummaryStep
	if len(keys)%CONFIG.SummaryStep != 0 {
		elNum++
	}
	if len(keys) < CONFIG.SummaryStep {
		elNum = len(keys)
	}
	sumKeys := make([]string, 0, elNum)
	sumOffsets := make([]int, 0, elNum)

	for i := 0; i < len(keys); i++ {
		key := keys[i]
		offset := offsets[i]
		value := append(SizeAndValueToBytes(key), IntToBytes(int64(offset))...)
		currBlock := fw.Write(value, false, nil)
		if i%CONFIG.SummaryStep == 0 {
			sumKeys = append(sumKeys, key)
			sumOffsets = append(sumOffsets, currBlock)
		}

	}
	return sumKeys, sumOffsets
}
func SerializeSummary(keys []string, offsets []int, fw file_writer.FileWriterInterface) {

	for i := 0; i < len(keys); i++ {
		key := keys[i]
		offset := offsets[i]
		value := append(SizeAndValueToBytes(key), IntToBytes(int64(offset))...)
		fw.Write(value, false, nil)

	}

}

func SerializeMetaData(summaryStartOffset int, bloomFilterBytes []byte, merkleTreeBytes []byte, numOfItems int, fw file_writer.FileWriterInterface, SummaryEndOffset int) {
	fw.Write(IntToBytes(int64(len(bloomFilterBytes))), false, nil)
	fw.Write(bloomFilterBytes, false, nil)
	fw.Write(IntToBytes(int64(summaryStartOffset)), false, nil)
	fw.Write(IntToBytes(int64(SummaryEndOffset)), false, nil)
	fw.Write(IntToBytes(int64(numOfItems)), false, nil)
	fw.Write(IntToBytes(int64(len(merkleTreeBytes))), false, nil)
	fw.Write(merkleTreeBytes, false, nil)
	metadataLength := 8 + len(bloomFilterBytes) + 8 + 8 + 8 + len(merkleTreeBytes) + 8
	fw.Write(nil, true, IntToBytes(int64(metadataLength)))

}

func IntToBytes(n int64) []byte {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, uint64(n))
	return buf
}

func SizeAndValueToBytes(value string) []byte {
	valueBytes := []byte(value)
	valueSizeBytes := IntToBytes(int64(len(valueBytes)))
	return append(valueSizeBytes, valueBytes...)
}
