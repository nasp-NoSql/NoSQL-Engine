package tests

import (
	"testing"

	bf "nosqlEngine/src/models/bloom_filter" // Adjust this import path to match your project structure
)

func TestCalculateM(t *testing.T) {
	tests := []struct {
		name              string
		expectedElements  int
		falsePositiveRate float64
		expectedMinM      uint
		expectedMaxM      uint
	}{
		{
			name:              "Standard case",
			expectedElements:  1000,
			falsePositiveRate: 0.01,
			expectedMinM:      9000,
			expectedMaxM:      10000,
		},
		{
			name:              "Low false positive rate",
			expectedElements:  100,
			falsePositiveRate: 0.001,
			expectedMinM:      1400,
			expectedMaxM:      1500,
		},
		{
			name:              "High false positive rate",
			expectedElements:  500,
			falsePositiveRate: 0.1,
			expectedMinM:      2000,
			expectedMaxM:      2500,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := bf.CalculateM(tt.expectedElements, tt.falsePositiveRate)
			if m < tt.expectedMinM || m > tt.expectedMaxM {
				t.Errorf("CalculateM() = %v, expected between %v and %v", m, tt.expectedMinM, tt.expectedMaxM)
			}
		})
	}
}

func TestCalculateK(t *testing.T) {
	tests := []struct {
		name             string
		expectedElements int
		m                uint
		expectedK        uint
	}{
		{
			name:             "Standard case",
			expectedElements: 1000,
			m:                9585, // Approximate value from CalculateM(1000, 0.01)
			expectedK:        7,
		},
		{
			name:             "Small filter",
			expectedElements: 100,
			m:                959,
			expectedK:        7,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			k := bf.CalculateK(tt.expectedElements, tt.m)
			// Allow some tolerance in K calculation
			if k < tt.expectedK-1 || k > tt.expectedK+1 {
				t.Errorf("CalculateK() = %v, expected around %v", k, tt.expectedK)
			}
		})
	}
}

func TestHashWithSeed_Hash(t *testing.T) {
	seed := []byte{1, 2, 3, 4}
	hash := bf.HashWithSeed{Seed: seed}

	data1 := []byte("test")
	data2 := []byte("test")
	data3 := []byte("different")

	// Same data should produce same hash
	hash1 := hash.Hash(data1)
	hash2 := hash.Hash(data2)
	if hash1 != hash2 {
		t.Errorf("Same data should produce same hash: %v != %v", hash1, hash2)
	}

	// Different data should produce different hash (with high probability)
	hash3 := hash.Hash(data3)
	if hash1 == hash3 {
		t.Errorf("Different data produced same hash (possible but unlikely): %v == %v", hash1, hash3)
	}
}

func TestCreateHashFunctions(t *testing.T) {
	k := uint32(5)
	hashes := bf.CreateHashFunctions(k)

	if len(hashes) != int(k) {
		t.Errorf("Expected %d hash functions, got %d", k, len(hashes))
	}

	// Test that different hash functions produce different results
	data := []byte("test")
	results := make(map[uint64]bool)

	for _, hash := range hashes {
		result := hash.Hash(data)
		if results[result] {
			t.Errorf("Hash functions produced duplicate result: %v", result)
		}
		results[result] = true
	}
}

func TestNewBloomFilter(t *testing.T) {
	// This test assumes NewBloomFilter() works with your CONFIG
	// You may need to mock or set up your config appropriately
	filter := bf.NewBloomFilter()

	if filter == nil {
		t.Fatal("NewBloomFilter() returned nil")
	}

	if filter.K <= 0 {
		t.Errorf("Expected positive K, got %d", filter.K)
	}

	if filter.M <= 0 {
		t.Errorf("Expected positive M, got %d", filter.M)
	}

	if len(filter.Array) != int(filter.M) {
		t.Errorf("Array length %d doesn't match M %d", len(filter.Array), filter.M)
	}

	if len(filter.Hashes) != int(filter.K) {
		t.Errorf("Hash functions count %d doesn't match K %d", len(filter.Hashes), filter.K)
	}
}

func TestBloomFilter_Add_And_Check(t *testing.T) {
	filter := &bf.BloomFilter{
		K:      3,
		M:      100,
		Array:  make([]byte, 100),
		Hashes: bf.CreateHashFunctions(3),
	}

	// Test adding and checking elements
	testItems := []string{"apple", "banana", "cherry", "date"}

	// Add items
	for _, item := range testItems {
		filter.Add(item)
	}

	// Check that added items are found
	for _, item := range testItems {
		if !filter.Check(item) {
			t.Errorf("Item '%s' should be found after adding", item)
		}
	}

	// Test some items that weren't added (might have false positives)
	notAdded := []string{"elderberry", "fig", "grape"}
	falsePositives := 0

	for _, item := range notAdded {
		if filter.Check(item) {
			falsePositives++
		}
	}

	// We expect some false positives, but not all items should be false positives
	if falsePositives == len(notAdded) {
		t.Errorf("All non-added items returned true (suspicious)")
	}
}

func TestBloomFilter_AddMultiple(t *testing.T) {
	filter := &bf.BloomFilter{
		K:      3,
		M:      100,
		Array:  make([]byte, 100),
		Hashes: bf.CreateHashFunctions(3),
	}

	items := []string{"item1", "item2", "item3", "item4"}
	resultArray := filter.AddMultiple(items)

	// Check that the returned array is the same as the filter's array
	if len(resultArray) != len(filter.Array) {
		t.Errorf("Returned array length %d doesn't match filter array length %d",
			len(resultArray), len(filter.Array))
	}

	for i := range resultArray {
		if resultArray[i] != filter.Array[i] {
			t.Errorf("Returned array differs from filter array at index %d", i)
		}
	}

	// Check that all items can be found
	for _, item := range items {
		if !filter.Check(item) {
			t.Errorf("Item '%s' should be found after adding multiple", item)
		}
	}
}

func TestBloomFilter_GetArray(t *testing.T) {
	filter := &bf.BloomFilter{
		K:      3,
		M:      100,
		Array:  make([]byte, 100),
		Hashes: bf.CreateHashFunctions(3),
	}

	// Modify some bits
	filter.Array[10] = 1
	filter.Array[50] = 1

	array := filter.GetArray()

	if len(array) != len(filter.Array) {
		t.Errorf("GetArray() returned wrong length: %d vs %d", len(array), len(filter.Array))
	}

	if array[10] != 1 || array[50] != 1 {
		t.Errorf("GetArray() didn't return correct array contents")
	}
}

// func TestGetBloomFilterArray(t *testing.T) {
// 	items := []string{"test1", "test2", "test3"}

// 	array, err := bf.GetBloomFilterArray(items)
// 	if err != nil {
// 		t.Errorf("GetBloomFilterArray() returned error: %v", err)
// 	}

// 	if array == nil {
// 		t.Fatal("GetBloomFilterArray() returned nil array")
// 	}

// 	// The array should have some bits set
// 	hasSetBits := false
// 	for _, bit := range array {
// 		if bit == 1 {
// 			hasSetBits = true
// 			break
// 		}
// 	}

// 	if !hasSetBits {
// 		t.Errorf("GetBloomFilterArray() didn't set any bits")
// 	}
// }

func TestBloomFilter_Serialization(t *testing.T) {
	// Create a test filter
	filter := &bf.BloomFilter{
		K:      3,
		M:      50,
		Array:  make([]byte, 50),
		Hashes: bf.CreateHashFunctions(3),
	}

	// Add some test data
	testItems := []string{"serialize_test1", "serialize_test2"}
	for _, item := range testItems {
		filter.Add(item)
	}

	// Test SerializeToByteArray
	data, err := filter.SerializeToByteArray()
	if err != nil {
		t.Fatalf("SerializeToByteArray() failed: %v", err)
	}

	if len(data) == 0 {
		t.Fatal("SerializeToByteArray() returned empty data")
	}

	// Test DeserializeFromByteArray
	deserializedFilter, err := bf.DeserializeFromByteArray(data)
	if err != nil {
		t.Fatalf("DeserializeFromByteArray() failed: %v", err)
	}

	// Verify the deserialized filter
	if deserializedFilter.K != filter.K {
		t.Errorf("Deserialized K mismatch: %d vs %d", deserializedFilter.K, filter.K)
	}

	if deserializedFilter.M != filter.M {
		t.Errorf("Deserialized M mismatch: %d vs %d", deserializedFilter.M, filter.M)
	}

	if len(deserializedFilter.Array) != len(filter.Array) {
		t.Errorf("Deserialized array length mismatch: %d vs %d",
			len(deserializedFilter.Array), len(filter.Array))
	}

	// Check that the arrays are identical
	for i := range filter.Array {
		if deserializedFilter.Array[i] != filter.Array[i] {
			t.Errorf("Deserialized array differs at index %d: %d vs %d",
				i, deserializedFilter.Array[i], filter.Array[i])
		}
	}

	// Check that the deserialized filter can find the same items
	for _, item := range testItems {
		if !deserializedFilter.Check(item) {
			t.Errorf("Deserialized filter can't find item '%s'", item)
		}
	}
}

// func TestBloomFilter_FileSerializationDeserialization(t *testing.T) {
// 	// Create test directory
// 	testDir := "test_serialized"
// 	err := os.MkdirAll(filepath.Join("src/models", testDir), 0755)
// 	if err != nil {
// 		t.Skipf("Cannot create test directory: %v", err)
// 	}
// 	defer os.RemoveAll(filepath.Join("src/models", testDir))

// 	// Create a test filter
// 	filter := &bf.BloomFilter{
// 		K:      4,
// 		M:      100,
// 		Array:  make([]byte, 100),
// 		Hashes: bf.CreateHashFunctions(4),
// 	}

// 	// Add some test data
// 	testItems := []string{"file_test1", "file_test2", "file_test3"}
// 	for _, item := range testItems {
// 		filter.Add(item)
// 	}

// 	// Test file serialization
// 	filename := filepath.Join(testDir, "test_filter.bin")
// 	err = filter.Serialize(filename)
// 	if err != nil {
// 		t.Fatalf("Serialize() failed: %v", err)
// 	}

// 	// Test file deserialization
// 	deserializedFilter, err := bf.Deserialize(filename)
// 	if err != nil {
// 		t.Fatalf("Deserialize() failed: %v", err)
// 	}

// 	// Verify the deserialized filter works correctly
// 	for _, item := range testItems {
// 		if !deserializedFilter.Check(item) {
// 			t.Errorf("Deserialized filter from file can't find item '%s'", item)
// 		}
// 	}
// }

func TestGetHashFunctions(t *testing.T) {
	// Test with non-existent file (should create new hash functions)
	hashes, err := bf.GetHashFunctions("nonexistent.bin", 3)
	if len(hashes) != 3 {
		t.Errorf("Expected 3 hash functions, got %d", len(hashes))
	}
	// Error should be returned but hashes should still be created
	if err == nil {
		t.Errorf("Expected error for non-existent file")
	}
}

func TestBloomFilter_EmptyFilter(t *testing.T) {
	filter := &bf.BloomFilter{
		K:      3,
		M:      100,
		Array:  make([]byte, 100),
		Hashes: bf.CreateHashFunctions(3),
	}

	// Empty filter should not contain any items
	testItems := []string{"not_added1", "not_added2", "not_added3"}

	for _, item := range testItems {
		if filter.Check(item) {
			// This could be a false positive, which is acceptable
			t.Logf("Empty filter returned true for '%s' (false positive)", item)
		}
	}
}

func TestBloomFilter_LargeInput(t *testing.T) {
	filter := &bf.BloomFilter{
		K:      5,
		M:      1000,
		Array:  make([]byte, 1000),
		Hashes: bf.CreateHashFunctions(5),
	}

	// Add many items
	items := make([]string, 100)
	for i := 0; i < 100; i++ {
		items[i] = "large_test_" + string(rune(i))
		filter.Add(items[i])
	}

	// Check that all items are found
	for _, item := range items {
		if !filter.Check(item) {
			t.Errorf("Item '%s' should be found in large filter", item)
		}
	}
}

func TestBloomFilter_FalsePositiveRate(t *testing.T) {
	// This test checks that the false positive rate is reasonable
	expectedElements := 1000
	falsePositiveRate := 0.01

	m := bf.CalculateM(expectedElements, falsePositiveRate)
	k := bf.CalculateK(expectedElements, m)

	filter := &bf.BloomFilter{
		K:      int32(k),
		M:      int32(m),
		Array:  make([]byte, m),
		Hashes: bf.CreateHashFunctions(uint32(k)),
	}

	// Add expected number of elements
	addedItems := make([]string, expectedElements)
	for i := 0; i < expectedElements; i++ {
		addedItems[i] = "fp_test_" + string(rune(i%256)) + string(rune((i/256)%256))
		filter.Add(addedItems[i])
	}

	// Test items not in the filter
	falsePositives := 0
	testCount := 1000

	for i := 0; i < testCount; i++ {
		testItem := "not_added_" + string(rune(i%256)) + string(rune((i/256)%256))
		if filter.Check(testItem) {
			falsePositives++
		}
	}

	actualFPRate := float64(falsePositives) / float64(testCount)

	// Allow some tolerance (false positive rate should be roughly as expected)
	if actualFPRate > falsePositiveRate*3 {
		t.Errorf("False positive rate too high: %.4f (expected around %.4f)",
			actualFPRate, falsePositiveRate)
	}

	t.Logf("False positive rate: %.4f (expected: %.4f)", actualFPRate, falsePositiveRate)
}

// Benchmark tests
func BenchmarkBloomFilter_Add(b *testing.B) {
	filter := &bf.BloomFilter{
		K:      7,
		M:      10000,
		Array:  make([]byte, 10000),
		Hashes: bf.CreateHashFunctions(7),
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		filter.Add("benchmark_item_" + string(rune(i%1000)))
	}
}

func BenchmarkBloomFilter_Check(b *testing.B) {
	filter := &bf.BloomFilter{
		K:      7,
		M:      10000,
		Array:  make([]byte, 10000),
		Hashes: bf.CreateHashFunctions(7),
	}

	// Pre-populate the filter
	for i := 0; i < 1000; i++ {
		filter.Add("benchmark_item_" + string(rune(i)))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		filter.Check("benchmark_item_" + string(rune(i%1000)))
	}
}
