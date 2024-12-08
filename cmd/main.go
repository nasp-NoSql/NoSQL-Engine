package main

import (
	"fmt"
	"merkle_tree_projekat/pkg/merkle_tree"
)

func main() {
	data := []string{"value1", "value2", "value3", "value4"}
	leafNodes := merkle_tree.CreateLeafNodes(data)
	root := merkle_tree.BuildMerkleTree(leafNodes)

	// Serijalizacija
	serialized, err := merkle_tree.SerializeMerkleTree(root)
	if err != nil {
		fmt.Println("Error during serialization:", err)
		return
	}

	// Deserijalizacija
	deserializedRoot, err := merkle_tree.DeserializeMerkleTree(serialized)
	if err != nil {
		fmt.Println("Error during deserialization:", err)
		return
	}

	// Proveri da li su originalni i deserijalizovani root isti
	fmt.Println("Original Root Hash:", root.Hash)
	fmt.Println("Deserialized Root Hash:", deserializedRoot.Hash)
	fmt.Println(deserializedRoot.Hash == root.Hash)
}
