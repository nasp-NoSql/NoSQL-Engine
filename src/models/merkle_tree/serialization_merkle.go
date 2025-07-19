package merkle_tree

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
)

func SerializeMerkleTree(root *Node) ([]byte, error) {
	var buf bytes.Buffer
	err := encodeNode(root, &buf)
	if err != nil {
		return nil, fmt.Errorf("failed to serialize Merkle tree: %v", err)
	}
	return buf.Bytes(), nil
}

func encodeNode(node *Node, w io.Writer) error {
	if node == nil {
		// Nil node: write 0 as length
		return binary.Write(w, binary.LittleEndian, uint32(0))
	}

	hashBytes := []byte(node.Hash)
	length := uint32(len(hashBytes))

	// Write hash length + hash content
	if err := binary.Write(w, binary.LittleEndian, length); err != nil {
		return err
	}
	if _, err := w.Write(hashBytes); err != nil {
		return err
	}

	// Recursively write left and right
	if err := encodeNode(node.Left, w); err != nil {
		return err
	}
	return encodeNode(node.Right, w)
}
