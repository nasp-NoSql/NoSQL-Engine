package merkle_tree

import (
	"fmt"
	"nosqlEngine/src/models/merkle_tree"
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

func BenchmarkCreateLeafNodes(b *testing.B) {
	for i := 0; i < b.N; i++ {
		merkle_tree.CreateLeafNodes([]string{"value1", "value2", "value3", "value4"})
	}
}

func BenchmarkCreateLeafNodesWithLargeData(b *testing.B) {
	data := make([]string, 100000000)
	for i := 0; i < len(data); i++ {
		data[i] = fmt.Sprintf("value%d", i)
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		merkle_tree.CreateLeafNodes(data)
	}
}
