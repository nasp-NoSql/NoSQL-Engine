package tests

import (
	"fmt"
	"nosqlEngine/src/models/hyperloglog"
	"os"
	"testing"
)

func TestHyperLogLogBasicEstimate(t *testing.T) {
	var hll hyperloglog.HyperLogLog
	err := hll.Initialize(0.01)
	if err != nil {
		t.Fatalf("Failed to initialize HLL: %v", err)
	}

	n := 100000
	for i := 0; i < n; i++ {
		data := []byte(fmt.Sprintf("item%d", i))
		hll.Add(data)
	}

	estimate := hll.Estimate()

	lower := uint64(float64(n) * 0.95)
	upper := uint64(float64(n) * 1.05)

	if estimate < lower || estimate > upper {
		t.Errorf("Estimate %d not within 5%% of %d", estimate, n)
	} else {
		t.Logf("HLL estimate: %d (expected ~%d)", estimate, n)
	}
}

func TestHyperLogLogSerialization(t *testing.T) {
	var hll hyperloglog.HyperLogLog
	err := hll.Initialize(0.01)
	if err != nil {
		t.Fatalf("Failed to initialize HLL: %v", err)
	}

	for i := 0; i < 1000; i++ {
		data := []byte(fmt.Sprintf("item%d", i))
		hll.Add(data)
	}

	filename := "hll_test.bin"
	defer os.Remove(filename)

	err = hll.Serialize(filename)
	if err != nil {
		t.Fatalf("Serialization failed: %v", err)
	}

	loaded, err := hyperloglog.Deserialize(filename)
	if err != nil {
		t.Fatalf("Deserialization failed: %v", err)
	}

	estimate1 := hll.Estimate()
	estimate2 := loaded.Estimate()

	if estimate1 != estimate2 {
		t.Errorf("Expected estimate %d after deserialization, got %d", estimate1, estimate2)
	}
}

func BenchmarkHyperLogLogAdd(b *testing.B) {
	var hll hyperloglog.HyperLogLog
	_ = hll.Initialize(0.01)

	data := []byte("benchmark_item")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		hll.Add(data)
	}
}

func BenchmarkHyperLogLogEstimate(b *testing.B) {
	var hll hyperloglog.HyperLogLog
	_ = hll.Initialize(0.01)

	for i := 0; i < 100000; i++ {
		data := []byte(fmt.Sprintf("item%d", i))
		hll.Add(data)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = hll.Estimate()
	}
}
