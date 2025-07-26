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

func CombineHashes(left, right string) string {
	combined := left + right
	return CalculateHash(combined)
}

func BuildStreamingMerkleTree(data []string) string {
	// ako broj elemenata nije paran, dupliciraj poslednji

	if len(data) == 1 {
		return CalculateHash(data[0])
	}
	if len(data)%2 != 0 && len(data) > 0 {
		data = append(data, data[len(data)-1])
	}

	var levels []string

	for _, datum := range data {
		currHash := CalculateHash(datum)
		level := 0

		for {
			if level >= len(levels) {
				levels = append(levels, "")
			}

			if levels[level] == "" {
				levels[level] = currHash
				break
			} else {
				currHash = CombineHashes(levels[level], currHash)
				levels[level] = ""
				level++
			}
		}
	}

	for i := len(levels) - 1; i >= 0; i-- {
		if levels[i] != "" {
			return levels[i]
		}
	}

	return ""
}

func BuildMerkleTree(nodes []*Node) *Node {
	if len(nodes) == 0 {
		panic("Cannot build Merkle tree with no nodes")
	}

	// Ako imamo samo jedan čvor, to je root
	if len(nodes) == 1 {
		return nodes[0]
	}

	var parentNodes []*Node

	// Ako broj čvorova nije paran, dupliramo poslednji
	if len(nodes)%2 != 0 {
		last := nodes[len(nodes)-1]
		nodes = append(nodes, &Node{Hash: last.Hash}) // duplirani čvor
	}

	for i := 0; i < len(nodes); i += 2 {
		left := nodes[i]
		right := nodes[i+1]

		parentHash := CalculateHash(left.Hash + right.Hash)
		parentNodes = append(parentNodes, &Node{
			Hash:  parentHash,
			Left:  left,
			Right: right,
		})
	}

	return BuildMerkleTree(parentNodes)
}

func GetMerkleTree(data []string) string {
	if len(data) == 0 {
		return ""
	}

	leafNodes := CreateLeafNodes(data)
	return BuildMerkleTree(leafNodes).Hash
}

func GetMerkleTreeFinal(data []string) string {
	if len(data) > 100000 {
		return BuildStreamingMerkleTree(data)
	} else {
		return GetMerkleTree(data)
	}
}

func BuildMerkleTreeFromBlocks(blocks [][]string) string {
	var roots []string

	for _, block := range blocks {
		root := GetMerkleTree(block)
		roots = append(roots, root)
	}

	return GetMerkleTree(roots)
}

// data = sstable.parse() tako nešto
func ValidateSSTable(sstable_data []string) (bool, error) {
	// 1. Učitaj Merkle root iz metapodataka

	// 2. Generiši novi Merkle root iz ulazne sstabele

	// 3. Uporedi Merkle root-ove
	// return storedRoot == recalculatedRoot, nil
	return sstable_data == nil, nil
}
