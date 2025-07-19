package merkle_tree

import (
	"bytes"
	"encoding/binary"
	"io"
)

func DeserializeMerkleTree(data []byte) (*Node, error) {
	reader := bytes.NewReader(data)
	return decodeNode(reader)
}

func decodeNode(r io.Reader) (*Node, error) {
	var length uint32

	// Read hash length
	if err := binary.Read(r, binary.LittleEndian, &length); err != nil {
		return nil, err
	}

	// Nil node
	if length == 0 {
		return nil, nil
	}

	// Read hash bytes
	hashBytes := make([]byte, length)
	if _, err := io.ReadFull(r, hashBytes); err != nil {
		return nil, err
	}

	node := &Node{Hash: string(hashBytes)}

	// Recursively read left and right children
	var err error
	node.Left, err = decodeNode(r)
	if err != nil {
		return nil, err
	}
	node.Right, err = decodeNode(r)
	if err != nil {
		return nil, err
	}

	return node, nil
}
