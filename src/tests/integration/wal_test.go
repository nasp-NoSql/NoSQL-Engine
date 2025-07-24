package integration

import (
	"fmt"
	w "nosqlEngine/src/storage/wal"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// Helper to create a temporary WAL for testing.
// It sets up the WAL to write into a temporary directory.
func setupTestWAL(t *testing.T, bufferSize, segmentSize int) (*w.WAL, string) {
	t.Helper()

	// Create a temporary directory for the test
	tempDir := t.TempDir()
	walDir := filepath.Join(tempDir, "wal")
	if err := os.Mkdir(walDir, 0755); err != nil {
		t.Fatalf("Failed to create temp WAL directory: %v", err)
	}

	// Override the default path generation to use our temp dir
	generateTestWALSegmentName := func() string {
		return filepath.Join(walDir, fmt.Sprintf("wal-%s.log", time.Now().Format("20060102-150405")))
	}

	// Create a WAL instance with our test configuration
	wal, err := w.NewWAL()
	if err != nil {
		t.Fatalf("NewWAL() failed: %v", err)
	}

	wal.SetBufferSize(bufferSize)
	wal.SetSegmentSize(segmentSize)
	wal.SetWriterLocation(generateTestWALSegmentName()) // Set initial file

	// Override path functions for replay testing
	getTestWALSegmentPaths := func() ([]string, error) {
		files, err := os.ReadDir(walDir)
		if err != nil {
			return nil, err
		}
		var paths []string
		for _, file := range files {
			if !file.IsDir() {
				paths = append(paths, filepath.Join(walDir, file.Name()))
			}
		}
		return paths, nil
	}

	// Replace the package-level functions with our test-specific ones
	w.GetWALSegmentPaths = getTestWALSegmentPaths

	return wal, walDir
}

func TestWAL_WriteAndReplay(t *testing.T) {
	wal, _ := setupTestWAL(t, 10, 1024) // Buffer size 10, segment size 1KB

	// Define some entries to write
	expectedEntries := []w.WALEntry{
		{Operation: "PUT", Key: "key1", Value: "value1"},
		{Operation: "DELETE", Key: "key2", Value: ""}, // Value should be empty for delete
		{Operation: "PUT", Key: "key3", Value: "some other value"},
	}

	// Write entries to the WAL
	if err := wal.WritePut(expectedEntries[0].Key, expectedEntries[0].Value); err != nil {
		t.Fatalf("WritePut failed: %v", err)
	}
	if err := wal.WriteDelete(expectedEntries[1].Key); err != nil {
		t.Fatalf("WriteDelete failed: %v", err)
	}
	if err := wal.WritePut(expectedEntries[2].Key, expectedEntries[2].Value); err != nil {
		t.Fatalf("WritePut failed: %v", err)
	}

	// Flush the buffer to disk
	if err := wal.Flush(); err != nil {
		t.Fatalf("Flush failed: %v", err)
	}

	// Replay the WAL
	replayedEntries, err := w.ReplayWAL()
	if err != nil {
		t.Fatalf("ReplayWAL failed: %v", err)
	}

	// --- Verification ---
	if len(replayedEntries) != len(expectedEntries) {
		t.Fatalf("Expected %d replayed entries, but got %d", len(expectedEntries), len(replayedEntries))
	}

	for i, replayed := range replayedEntries {
		expected := expectedEntries[i]
		if replayed.Operation != expected.Operation || replayed.Key != expected.Key || replayed.Value != expected.Value {
			t.Errorf("Entry mismatch at index %d.\nExpected: %+v\nGot:      %#v", i, expected, replayed)
		}
	}
}

func TestWAL_Rotation(t *testing.T) {
	// Use a very small segment size to force rotation
	wal, walDir := setupTestWAL(t, 2, 100) // Rotate after ~100 bytes

	// Write enough entries to trigger a rotation
	for i := 0; i < 5; i++ {
		key := fmt.Sprintf("key-%d", i)
		value := "a-long-value-to-make-sure-we-exceed-the-segment-size-quickly"
		if err := wal.WritePut(key, value); err != nil {
			t.Fatalf("WritePut failed: %v", err)
		}
	}
	// The buffer is size 2, so flush will be called multiple times, triggering rotation checks.

	// Check how many WAL files were created
	files, err := os.ReadDir(walDir)
	if err != nil {
		t.Fatalf("Could not read test WAL directory: %v", err)
	}

	if len(files) < 2 {
		t.Errorf("Expected at least 2 WAL segment files after rotation, but found %d", len(files))
	}

	// Replay and ensure all data is recovered from all segments
	replayedEntries, err := w.ReplayWAL()
	if err != nil {
		t.Fatalf("ReplayWAL failed after rotation: %v", err)
	}

	if len(replayedEntries) != 5 {
		t.Fatalf("Expected 5 entries after replay, but got %d", len(replayedEntries))
	}
}

func TestWAL_BufferFlush(t *testing.T) {
	wal, walDir := setupTestWAL(t, 5, 1024)

	// Write fewer entries than buffer size
	wal.WritePut("key1", "value1")
	wal.WritePut("key2", "value2")

	// Get file info. Should not exist yet or be empty, as Flush hasn't been called.
	files, _ := os.ReadDir(walDir)
	info, _ := os.Stat(files[0].Name())
	if info.Size() > 0 {
		t.Errorf("WAL file should be empty before buffer is full or flushed, but has size %d", info.Size())
	}

	// Write more entries to fill the buffer and trigger an automatic flush
	wal.WritePut("key3", "value3")
	wal.WritePut("key4", "value4")
	wal.WritePut("key5", "value5") // This should trigger flush

	// Now check the file size again
	info, _ = os.Stat(files[0].Name())
	if info.Size() == 0 {
		t.Error("WAL file should have content after buffer filled, but is empty")
	}
}
