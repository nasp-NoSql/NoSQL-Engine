package merkle_tree

import (
	"fmt"
	"testing"
)

func TestCreateLeafNodes(t *testing.T) {
	data := []string{"value1", "value2", "value3", "value4"}

	leafNodes := CreateLeafNodes(data)
	if len(leafNodes) != len(data) {
		t.Errorf("Expected %d leaf nodes, got %d", len(data), len(leafNodes))
	}

	for i, node := range leafNodes {
		expectedHash := CalculateHash(data[i])
		if node.Hash != expectedHash {
			t.Errorf("Expected hash %s, got %s", expectedHash, node.Hash)
		}
	}
}

func TestSerializeDeserializeMerkleTree(t *testing.T) {
	data := []string{"value1", "value2", "value3", "value4"}
	leafNodes := CreateLeafNodes(data)
	root := BuildMerkleTree(leafNodes)

	serialized, err := SerializeMerkleTree(root)
	if err != nil {
		t.Fatalf("Error during serialization: %v", err)
	}

	deserializedRoot, err := DeserializeMerkleTree(serialized)
	if err != nil {
		t.Fatalf("Error during deserialization: %v", err)
	}

	if root.Hash != deserializedRoot.Hash {
		t.Errorf("Expected root hash %s, got %s", root.Hash, deserializedRoot.Hash)
	}
}

func BenchmarkCreateLeafNodes(b *testing.B) {
	for i := 0; i < b.N; i++ {
		CreateLeafNodes([]string{"value1", "value2", "value3", "value4"})
	}
}

func BenchmarkCreateLeafNodesWithLargeData(b *testing.B) {
	data := make([]string, 100000000)
	for i := 0; i < len(data); i++ {
		data[i] = fmt.Sprintf("value%d", i)
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		CreateLeafNodes(data)
	}
}
