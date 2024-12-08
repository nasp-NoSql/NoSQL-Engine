package merkle_tree

import (
	"bytes"
	"encoding/gob"
)

func DeserializeMerkleTree(data []byte) (*Node, error) {
	buf := bytes.NewBuffer(data)
	decoder := gob.NewDecoder(buf)
	return DecodeNode(decoder)
}

func DecodeNode(decoder *gob.Decoder) (*Node, error) {
	var hash string
	err := decoder.Decode(&hash)
	if err != nil {
		return nil, err
	}
	if hash == "" {
		return nil, nil
	}

	node := &Node{Hash: hash}
	node.Left, err = DecodeNode(decoder)
	if err != nil {
		return nil, err
	}
	node.Right, err = DecodeNode(decoder)
	if err != nil {
		return nil, err
	}
	return node, nil
}
