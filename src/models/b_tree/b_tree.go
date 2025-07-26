package b_tree

import (
	"encoding/gob"
	"os"
	"path/filepath"
)

// BTreeNode represents a node in the B-Tree
type BTreeNode struct {
	Keys     []string
	Values   []string
	Children []*BTreeNode
	IsLeaf   bool
}

// BTree represents the B-Tree itself
// t is the minimum degree (defines the range for number of keys)
type BTree struct {
	Root *BTreeNode
	T    int // minimum degree
	Size int // number of keys in the tree
}

// NewBTree creates a new B-Tree with given minimum degree
func NewBTree(t int) *BTree {
	return &BTree{Root: &BTreeNode{IsLeaf: true}, T: t}
}

func (tree *BTree) Get(key string) (string, bool) {
	return tree.Root.search(key)
}

func (node *BTreeNode) search(key string) (string, bool) {
	i := 0
	for i < len(node.Keys) && key > node.Keys[i] {
		i++
	}
	if i < len(node.Keys) && key == node.Keys[i] {
		if node.Values[i] == "<TOMBSTONE!>" {
			return "", false
		}
		return node.Values[i], true
	}
	if node.IsLeaf {
		return "", false
	}
	return node.Children[i].search(key)
}

func (tree *BTree) Add(key, value string) {
	// Check if key already exists (including tombstoned keys)
	if tree.updateExistingKey(key, value) {
		return
	}

	// Add new key
	root := tree.Root
	if len(root.Keys) == 2*tree.T-1 {
		s := &BTreeNode{IsLeaf: false, Children: []*BTreeNode{root}}
		tree.Root = s
		s.splitChild(0, tree.T)
		s.addNonFull(key, value, tree.T)
	} else {
		root.addNonFull(key, value, tree.T)
	}
	tree.Size++
}

// updateExistingKey updates an existing key (including tombstoned ones) and returns true if key was found
func (tree *BTree) updateExistingKey(key, value string) bool {
	return tree.Root.updateExistingKeyRecursive(key, value)
}

func (node *BTreeNode) updateExistingKeyRecursive(key, value string) bool {
	i := 0
	for i < len(node.Keys) && key > node.Keys[i] {
		i++
	}
	if i < len(node.Keys) && key == node.Keys[i] {
		node.Values[i] = value
		return true
	}
	if !node.IsLeaf {
		return node.Children[i].updateExistingKeyRecursive(key, value)
	}
	return false
}

func (node *BTreeNode) addNonFull(key, value string, t int) {
	i := len(node.Keys) - 1
	if node.IsLeaf {
		node.Keys = append(node.Keys, "")
		node.Values = append(node.Values, "")
		for i >= 0 && key < node.Keys[i] {
			node.Keys[i+1] = node.Keys[i]
			node.Values[i+1] = node.Values[i]
			i--
		}
		node.Keys[i+1] = key
		node.Values[i+1] = value
		return
	}

	for i >= 0 && key < node.Keys[i] {
		i--
	}
	i++
	if len(node.Children[i].Keys) == 2*t-1 {
		node.splitChild(i, t)
		if key > node.Keys[i] {
			i++
		}
	}
	node.Children[i].addNonFull(key, value, t)
}

func (node *BTreeNode) splitChild(i, t int) {
	y := node.Children[i]
	z := &BTreeNode{IsLeaf: y.IsLeaf}
	z.Keys = append(z.Keys, y.Keys[t:]...)
	z.Values = append(z.Values, y.Values[t:]...)
	y.Keys = y.Keys[:t-1]
	y.Values = y.Values[:t-1]
	if !y.IsLeaf {
		z.Children = append(z.Children, y.Children[t:]...)
		y.Children = y.Children[:t]
	}
	node.Children = append(node.Children, nil)
	copy(node.Children[i+2:], node.Children[i+1:])
	node.Children[i+1] = z
	node.Keys = append(node.Keys, "")
	node.Values = append(node.Values, "")
	copy(node.Keys[i+1:], node.Keys[i:])
	copy(node.Values[i+1:], node.Values[i:])
	node.Keys[i] = y.Keys[t-1]
	node.Values[i] = y.Values[t-1]
}

// Remove sets the value of the key to "<TOMBSTONE!>" if found (logical removal)
func (tree *BTree) Remove(key string) {
	tree.Root.Remove(key)
}

func (node *BTreeNode) Remove(key string) {
	i := 0
	for i < len(node.Keys) && key > node.Keys[i] {
		i++
	}
	if i < len(node.Keys) && key == node.Keys[i] {
		node.Values[i] = "<TOMBSTONE!>"
		return
	}
	if node.IsLeaf {
		return
	}
	node.Children[i].Remove(key)
}

// Serialize serializes the B-Tree into a byte slice
func (tree *BTree) Serialize(filename string) error {
	path := filepath.Join("src/models/serialized", filename)
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()
	enc := gob.NewEncoder(file)
	return enc.Encode(tree)
}

// Deserialize deserializes a byte slice into a B-Tree
func Deserialize(filename string) (*BTree, error) {
	path := filepath.Join("src/models/serialized", filename)
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	dec := gob.NewDecoder(file)
	var tree BTree
	if err := dec.Decode(&tree); err != nil {
		return nil, err
	}
	return &tree, nil
}
