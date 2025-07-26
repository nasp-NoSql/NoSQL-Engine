package tests

import (
	"fmt"
	"nosqlEngine/src/models/merkle_tree"
	"strconv"
	"testing"
)

func TestCreateLeafNodes(t *testing.T) {
	data := []string{"value1", "value2", "value3", "value4"}

	leafNodes := merkle_tree.CreateLeafNodes(data)
	if len(leafNodes) != len(data) {
		t.Errorf("Expected %d leaf nodes, got %d", len(data), len(leafNodes))
	}

	for i, node := range leafNodes {
		expectedHash := merkle_tree.CalculateHash(data[i])
		if node.Hash != expectedHash {
			t.Errorf("Expected hash %s, got %s", expectedHash, node.Hash)
		}
	}
}

func TestSerializeDeserializeMerkleTree(t *testing.T) {
	data := []string{"value1", "value2", "value3", "value4"}
	leafNodes := merkle_tree.CreateLeafNodes(data)
	root := merkle_tree.BuildMerkleTree(leafNodes)

	serialized, err := merkle_tree.SerializeMerkleTree(root)
	if err != nil {
		t.Fatalf("Error during serialization: %v", err)
	}

	deserializedRoot, err := merkle_tree.DeserializeMerkleTree(serialized)
	if err != nil {
		t.Fatalf("Error during deserialization: %v", err)
	}

	if root.Hash != deserializedRoot.Hash {
		t.Errorf("Expected root hash %s, got %s", root.Hash, deserializedRoot.Hash)
	}
}

func TestStreamingVsClassicMerkleTree(t *testing.T) {
	testCases := [][]string{
		{},              // Prazna lista
		{"a"},           // Jedan element
		{"a", "b"},      // Paran broj elemenata
		{"a", "b", "c"}, // Neparan broj
		{"key1:value1", "key2:value2", "key3:value3"}, // Simulacija key-value parova
	}

	for i, data := range testCases {
		classic := merkle_tree.GetMerkleTree(data)
		streaming := merkle_tree.BuildStreamingMerkleTree(data)

		if classic != streaming {
			t.Errorf("Test %d failed: Merkle roots differ!\nClassic:   %s\nStreaming: %s\nData: %v", i, classic, streaming, data)
		}
	}
}

func TestStreamingVsClassicMerkleTree_OnlyPowerOfTwo(t *testing.T) {
	testCases := [][]string{
		{},                   // Prazno
		{"a"},                // 1
		{"a", "b"},           // 2
		{"a", "b", "c", "d"}, // 4
		{"k1:v1", "k2:v2", "k3:v3", "k4:v4", "k5:v5", "k6:v6", "k7:v7", "k8:v8"}, // 8
	}

	for i, data := range testCases {
		classic := merkle_tree.GetMerkleTree(data)
		streaming := merkle_tree.BuildStreamingMerkleTree(data)

		if classic != streaming {
			t.Errorf("Test %d failed: Merkle roots differ!\nClassic:   %s\nStreaming: %s\nData: %v", i, classic, streaming, data)
		}
	}
}

func BenchmarkCreateLeafNodes(b *testing.B) {
	for i := 0; i < b.N; i++ {
		merkle_tree.CreateLeafNodes([]string{"value1", "value2", "value3", "value4"})
	}
}

func BenchmarkCreateLeafNodesWithLargeData(b *testing.B) {
	data := make([]string, 1000000)
	for i := 0; i < len(data); i++ {
		data[i] = fmt.Sprintf("value%d", i)
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		merkle_tree.CreateLeafNodes(data)
	}
}

func generateTestData(n int) []string {
	data := make([]string, n)
	for i := 0; i < n; i++ {
		data[i] = "key" + strconv.Itoa(i) + ":value" + strconv.Itoa(i)
	}
	return data
}

func BenchmarkClassicMerkleTree(b *testing.B) {
	data := generateTestData(1000000) // 100k elemenata

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		merkle_tree.GetMerkleTree(data)
	}
}

func BenchmarkStreamingMerkleTree(b *testing.B) {
	data := generateTestData(1000000)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		merkle_tree.BuildStreamingMerkleTree(data)
	}
}
