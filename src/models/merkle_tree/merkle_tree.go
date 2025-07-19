package merkle_tree

import (
	"crypto/sha256"
	"encoding/hex"
)

type Node struct {
	Left  *Node
	Right *Node
	Hash  string
}

func CalculateHash(datum string) string {
	hash := sha256.Sum256([]byte(datum))
	return hex.EncodeToString(hash[:])
}

func CreateLeafNodes(data []string) []*Node {
	var leafNodes []*Node

	for _, d := range data {
		hash := CalculateHash(d)
		leafNodes = append(leafNodes, &Node{Hash: hash})
	}

	return leafNodes
}

func BuildMerkleTree(nodes []*Node) *Node {
	if len(nodes) == 0 {
		panic("Cannot build Merkle tree with no nodes")
	}

	if len(nodes) == 1 {
		return nodes[0] // Ako postoji samo jedan čvor, on je koren
	}

	var parentNodes []*Node

	for i := 0; i < len(nodes); i += 2 {
		var rightNode *Node
		var rightHash string
		if i+1 < len(nodes) {
			rightNode = nodes[i+1]
			rightHash = rightNode.Hash
		}

		parentHash := CalculateHash(nodes[i].Hash + rightHash)
		parentNodes = append(parentNodes, &Node{
			Hash:  parentHash,
			Left:  nodes[i],
			Right: rightNode,
		})
	}

	return BuildMerkleTree(parentNodes)
}

func GetMerkleTree(data []string) string {
	leafNodes := CreateLeafNodes(data)
	return BuildMerkleTree(leafNodes).Hash
}

// data = sstable.parse() tako nešto
func ValidateSSTable(sstable_data []string) (bool, error) {
	// 1. Učitaj Merkle root iz metapodataka

	// 2. Generiši novi Merkle root iz ulazne sstabele

	// 3. Uporedi Merkle root-ove
	// return storedRoot == recalculatedRoot, nil
	return sstable_data == nil, nil
}
