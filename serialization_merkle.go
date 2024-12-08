package merkle_tree

import (
	"bytes"
	"encoding/gob"
	"fmt"
)

func SerializeMerkleTree(root *Node) ([]byte, error) {
	var buf bytes.Buffer
	encoder := gob.NewEncoder(&buf)
	err := EncodeNode(root, encoder)
	if err != nil {
		return nil, fmt.Errorf("failed to serialize Merkle tree: %v", err)
	}
	return buf.Bytes(), nil
}

func EncodeNode(node *Node, encoder *gob.Encoder) error {
	if node == nil {
		return encoder.Encode("")
	}

	err := encoder.Encode(node.Hash)
	if err != nil {
		return err
	}
	err = EncodeNode(node.Left, encoder)
	if err != nil {
		return err
	}
	return EncodeNode(node.Right, encoder)
}
