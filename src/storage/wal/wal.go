package wal

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"nosqlEngine/src/service/file_writer"
	"os"
	"path/filepath"
	"time"
)

// WALEntry represents a single log entry in the WAL
// Operation: "PUT" or "DELETE"
type WALEntry struct {
	Operation string
	Key       string
	Value     string // empty for DELETE
	Timestamp int64  // seconds since epoch
}

// WAL handles writing to the write-ahead log file with a buffer pool and supports rotation/archiving
// Usage: wal, _ := NewWAL("data/wal/wal.log", 100)
//
//	wal.Rotate("data/wal/wal-20250625.log")
//	wal.Archive("data/wal/wal-20250625.log", "data/wal/archive/wal-20250625.log")
//	wal.Delete("data/wal/wal-20250625.log")
type WAL struct {
	file   *os.File
	buffer [][]byte                // changed from []string to [][]byte
	size   int                     // buffer pool size
	writer *file_writer.FileWriter // add FileWriter for block writing
}

// NewWAL creates or opens a WAL file for appending, with a buffer pool of given size
func NewWAL(path string, bufferSize int, writer *file_writer.FileWriter) (*WAL, error) {
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, err
	}
	return &WAL{file: f, buffer: make([][]byte, 0, bufferSize), size: bufferSize, writer: writer}, nil
}

// encodeWALEntry encodes a WALEntry into the binary WAL format
func encodeWALEntry(entry WALEntry) ([]byte, error) {
	keyBytes := []byte(entry.Key)
	valueBytes := []byte(entry.Value)
	keySize := uint64(len(keyBytes))
	valueSize := uint64(len(valueBytes))
	var tombstone byte = 0
	if entry.Operation == "DELETE" {
		tombstone = 1
	}
	buf := new(bytes.Buffer)
	// Reserve space for CRC (4 bytes)
	buf.Write(make([]byte, 4))
	// Timestamp (16 bytes, use int64 seconds, pad to 16)
	ts := make([]byte, 16)
	binary.LittleEndian.PutUint64(ts, uint64(entry.Timestamp))
	buf.Write(ts)
	// Tombstone (1 byte)
	buf.WriteByte(tombstone)
	// Key Size (8 bytes)
	ks := make([]byte, 8)
	binary.LittleEndian.PutUint64(ks, keySize)
	buf.Write(ks)
	// Value Size (8 bytes)
	vs := make([]byte, 8)
	binary.LittleEndian.PutUint64(vs, valueSize)
	buf.Write(vs)
	// Key
	buf.Write(keyBytes)
	// Value
	buf.Write(valueBytes)
	// Compute CRC over everything except the first 4 bytes
	crc := crc32.ChecksumIEEE(buf.Bytes()[4:])
	binary.LittleEndian.PutUint32(buf.Bytes()[0:4], crc)
	return buf.Bytes(), nil
}

// WritePut logs a PUT operation to the WAL buffer
func (w *WAL) WritePut(key, value string) error {
	entry := WALEntry{
		Operation: "PUT",
		Key:       key,
		Value:     value,
		Timestamp: time.Now().Unix(),
	}
	data, err := encodeWALEntry(entry)
	if err != nil {
		return err
	}
	w.buffer = append(w.buffer, data)
	if len(w.buffer) >= w.size {
		return w.Flush()
	}
	return nil
}

// WriteDelete logs a DELETE operation to the WAL buffer
func (w *WAL) WriteDelete(key string) error {
	entry := WALEntry{
		Operation: "DELETE",
		Key:       key,
		Value:     "",
		Timestamp: time.Now().Unix(),
	}
	data, err := encodeWALEntry(entry)
	if err != nil {
		return err
	}
	w.buffer = append(w.buffer, data)
	if len(w.buffer) >= w.size {
		return w.Flush()
	}
	return nil
}

// Flush writes the buffer to disk and clears it
func (w *WAL) Flush() error {
	if len(w.buffer) == 0 {
		return nil
	}
	for _, entry := range w.buffer {
		if w.writer != nil {
			w.writer.Write(entry, false)
		} else {
			if _, err := w.file.Write(entry); err != nil {
				return err
			}
		}
	}
	w.buffer = w.buffer[:0]
	return nil
}

// Close flushes the buffer and closes the WAL file
func (w *WAL) Close() error {
	if err := w.Flush(); err != nil {
		return err
	}
	return w.file.Close()
}

// Rotate closes the current WAL and starts a new one at newPath
func (w *WAL) Rotate(newPath string, bufferSize int) error {
	if err := w.Close(); err != nil {
		return err
	}
	f, err := os.OpenFile(newPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	w.file = f
	w.buffer = make([][]byte, 0, bufferSize)
	w.size = bufferSize
	return nil
}

// Archive moves a WAL file to an archive directory
func Archive(srcPath, archivePath string) error {
	if err := os.MkdirAll(filepath.Dir(archivePath), 0755); err != nil {
		return err
	}
	src, err := os.Open(srcPath)
	if err != nil {
		return err
	}
	defer src.Close()
	archive, err := os.Create(archivePath)
	if err != nil {
		return err
	}
	defer archive.Close()
	_, err = io.Copy(archive, src)
	if err != nil {
		return err
	}
	return nil
}

// Delete removes a WAL file from disk
func Delete(path string) error {
	return os.Remove(path)
}

// Helper to generate a rotated WAL filename with timestamp

func RotatedWALName() string {
	return fmt.Sprintf("data/wal/wal-%s.log", time.Now().Format("20060102-150405"))
}

// Helper to read and parse a single WAL entry from the file
func readWALEntry(r io.Reader) (*WALEntry, uint32, []byte, error) {
	head := make([]byte, 4+16+1+8+8) // CRC + Timestamp + Tombstone + KeySize + ValueSize
	_, err := io.ReadFull(r, head)
	if err != nil {
		return nil, 0, nil, err
	}
	crc := binary.LittleEndian.Uint32(head[0:4])
	ts := int64(binary.LittleEndian.Uint64(head[4:12]))
	tombstone := head[20]
	keySize := binary.LittleEndian.Uint64(head[21:29])
	valueSize := binary.LittleEndian.Uint64(head[29:37])
	key := make([]byte, keySize)
	if _, err := io.ReadFull(r, key); err != nil {
		return nil, 0, nil, err
	}
	value := make([]byte, valueSize)
	if _, err := io.ReadFull(r, value); err != nil {
		return nil, 0, nil, err
	}
	payload := append(head[4:], key...)
	payload = append(payload, value...)
	op := "PUT"
	if tombstone == 1 {
		op = "DELETE"
	}
	entry := &WALEntry{
		Operation: op,
		Key:       string(key),
		Value:     string(value),
		Timestamp: ts,
	}
	return entry, crc, payload, nil
}

// Replay reads the WAL file and returns all entries (for recovery)
func Replay(path string) ([]WALEntry, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var entries []WALEntry
	for {
		entry, crc, payload, err := readWALEntry(f)
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		// Validate CRC
		if crc32.ChecksumIEEE(payload) != crc {
			return nil, fmt.Errorf("WAL entry CRC mismatch")
		}
		entries = append(entries, *entry)
	}
	return entries, nil
}
