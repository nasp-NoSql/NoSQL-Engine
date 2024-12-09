package wal

import (
	"binary"
	"fmt"
	"os"
)

/*
   +---------------+-----------------+---------------+---------------+-----------------+-...-+--...--+
   |    CRC (4B)   | Timestamp (8B) | Tombstone(1B) | Key Size (8B) | Value Size (8B) | Key | Value |
   +---------------+-----------------+---------------+---------------+-----------------+-...-+--...--+
   CRC = 32bit hash computed over the payload using CRC
   Key Size = Length of the Key data
   Tombstone = If this record was deleted and has a value
   Value Size = Length of the Value data
   Key = Key data
   Value = Value data
   Timestamp = Timestamp of the operation in seconds
*/

const (
	CRC_SIZE        = 4
	TIMESTAMP_SIZE  = 8
	TOMBSTONE_SIZE  = 1
	KEY_SIZE_SIZE   = 8
	VALUE_SIZE_SIZE = 8

	CRC_START        = 0
	TIMESTAMP_START  = CRC_START + CRC_SIZE
	TOMBSTONE_START  = TIMESTAMP_START + TIMESTAMP_SIZE
	KEY_SIZE_START   = TOMBSTONE_START + TOMBSTONE_SIZE
	VALUE_SIZE_START = KEY_SIZE_START + KEY_SIZE_SIZE
	KEY_START        = VALUE_SIZE_START + VALUE_SIZE_SIZE

	WAL_BLOCK_SIZE   = 8192
	WAL_SEGMENT_SIZE = 16777216
)

type WALRecord struct {
	CRC       uint32
	Timestamp string
	Tombstone bool
	KeySize   uint64
	ValueSize uint64
	Key       []byte
	Value     []byte
}

func NewRecord(CRC uint32, TimeStamp string, Tombstone bool, Key string, Value string) *WALRecord {
	return &WALRecord{
		CRC:       CRC,
		Timestamp: TimeStamp,
		Tombstone: Tombstone,
		KeySize:   uint64(len(Key)),
		ValueSize: uint64(len(Value)),
		Key:       []byte(Key),
		Value:     []byte(Value),
	}
}

func (rec WALRecord) len() uint64 {
	size := CRC_SIZE + TIMESTAMP_SIZE + TOMBSTONE_SIZE + KEY_SIZE_SIZE + VALUE_SIZE_SIZE + rec.KeySize + rec.ValueSize
	return size
}

func Serialize(rec WALRecord) []byte {
	data := make([]byte, 0)
	crcBytes := make([]byte, CRC_SIZE)
	binary.LittleEndian.PutUint32(crcBytes, rec.CRC)
	data = append(data, crcBytes...)
	timestampBytes := make([]byte, TIMESTAMP_SIZE)
	binary.LittleEndian.PutUint64(timestampBytes, rec.Timestamp)
	data = append(data, timestampBytes...)
	if rec.Tombstone {
		data = append(data, 1)
	} else {
		data = append(data, 0)
	}
	keySizeBytes := make([]byte, KEY_SIZE_SIZE)
	binary.LittleEndian.PutUint64(keySizeBytes, rec.KeySize)
	data = append(data, keySizeBytes...)
	valueSizeBytes := make([]byte, VALUE_SIZE_SIZE)
	binary.LittleEndian.PutUint64(valueSizeBytes, rec.ValueSize)
	data = append(data, valueSizeBytes...)
	data = append(data, rec.Key...)
	data = append(data, rec.Value...)
	return data
}

type Block struct {
	Type []byte
	Data []byte
}

func MakeBlocks(recType string, data []byte) []Block {
	blocks := make([]Block, 0)
	if recType == "full" {
		block := Block{
			Type: []byte{11},
			Data: data,
		}
		blocks = append(blocks, block)
		return blocks
	}
	block := Block{
		Type: []byte{10},
		Data: data[:WAL_BLOCK_SIZE-2],
	}
	var pointer int
	blocks = append(blocks, block)
	for i := 2; len(data) > i*WAL_BLOCK_SIZE; i++ {
		block = Block{
			Type: []byte{00},
			Data: data[(i-1)*(WAL_BLOCK_SIZE-2) : i*(WAL_BLOCK_SIZE-2)],
		}
		blocks = append(blocks, block)
		pointer = i * (WAL_BLOCK_SIZE - 2)
	}
	block = Block{
		Type: []byte{01},
		Data: data[pointer:],
	}
	blocks = append(blocks, block)
	return blocks
}

type WALSegment struct {
	File        os.File
	FilePath    string
	Blocks      []Block
	LastBlockID int
}

func NewSegment(path string) *WALSegment {
	file, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil
	}
	defer file.Close()
	return &WALSegment{
		File:     *file,
		FilePath: path,
		Blocks:   make([]Block, 0, WAL_SEGMENT_SIZE/WAL_BLOCK_SIZE),
	}
}

type WAL struct {
	Records    []WALRecord
	Segments   []WALSegment
	MaxSeg     int
	CurrentSeg *WALSegment
}

func NewWAL(n int) *WAL {
	return &WAL{
		Records:    make([]WALRecord, 0),
		Segments:   make([]WALSegment, 0, n),
		MaxSeg:     n,
		CurrentSeg: NewSegment(fmt.Sprintf("wal_segment_%d.log", 0)),
	}
}

func (w *WAL) addRecord(rec WALRecord) error {
	w.Records = append(w.Records, rec)
	data := Serialize(rec)
	var blocks []Block
	if rec.len() < WAL_BLOCK_SIZE-2 {
		blocks = MakeBlocks("full", data)
	} else {
		blocks = MakeBlocks("", data)
	}
	numOfBlocks := WAL_SEGMENT_SIZE / WAL_BLOCK_SIZE
	for i := 0; i < len(blocks); i++ {
		duzina := len(w.CurrentSeg.Blocks) + 1
		for duzina < numOfBlocks {
			w.CurrentSeg.Blocks = append(w.CurrentSeg.Blocks, blocks[i])
			w.CurrentSeg.File.Write(blocks[i].Data)
			duzina++
			continue
		}
		w.CurrentSeg.LastBlockID = duzina - 1
		w.Segments = append(w.Segments, *w.CurrentSeg)
		if cap(w.Segments) == w.MaxSeg {
			w.Segments = w.Segments[1:]
		}
		w.CurrentSeg = NewSegment(fmt.Sprintf("wal_segment_%d.log", len(w.Segments)))
	}
	return nil
}
