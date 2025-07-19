package tests

import (
	"nosqlEngine/src/models/countmin_sketch"
	"testing"
)

func TestCountMinSketchBasicCounts(t *testing.T) {
	var cms countmin_sketch.CountMinSketch
	cms.Initialize(0.01, 0.001) // 1% error, 99.9% confidence

	keys := []string{"apple", "banana", "apple", "orange", "banana", "apple"}

	for _, key := range keys {
		cms.Add([]byte(key))
	}

	appleCount := cms.Estimate([]byte("apple"))
	if appleCount < 3 {
		t.Errorf("Expected at least 3 for 'apple', got %d", appleCount)
	}

	bananaCount := cms.Estimate([]byte("banana"))
	if bananaCount < 2 {
		t.Errorf("Expected at least 2 for 'banana', got %d", bananaCount)
	}

	orangeCount := cms.Estimate([]byte("orange"))
	if orangeCount < 1 {
		t.Errorf("Expected at least 1 for 'orange', got %d", orangeCount)
	}

	pearCount := cms.Estimate([]byte("pear"))
	t.Logf("Count for 'pear': %d", pearCount)
}

func TestCountMinSketchAccuracy(t *testing.T) {
	var cms countmin_sketch.CountMinSketch
	cms.Initialize(0.01, 0.001)

	key := []byte("testkey")
	insertions := 1000000

	for i := 0; i < insertions; i++ {
		cms.Add(key)
	}

	estimate := cms.Estimate(key)

	if estimate < uint(insertions) {
		t.Errorf("Estimate %d is less than insertions %d", estimate, insertions)
	}

	// The estimate should not exceed insertions by more than 1% + epsilon tolerance
	tolerance := uint(float64(insertions) * 0.02)
	if estimate > uint(insertions)+tolerance {
		t.Errorf("Estimate %d exceeds tolerance %d", estimate, uint(insertions)+tolerance)
	}
}

func BenchmarkCountMinSketchAdd(b *testing.B) {
	var cms countmin_sketch.CountMinSketch
	cms.Initialize(0.01, 0.001)

	data := []byte("benchmark_key")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cms.Add(data)
	}
}

func BenchmarkCountMinSketchEstimate(b *testing.B) {
	var cms countmin_sketch.CountMinSketch
	cms.Initialize(0.01, 0.001)

	data := []byte("benchmark_key")
	for i := 0; i < 100000; i++ {
		cms.Add(data)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cms.Estimate(data)
	}
}
