package b_tree

// BTreeNode represents a node in the B-Tree
type BTreeNode struct {
	keys     []string
	values   []string
	children []*BTreeNode
	isLeaf   bool
}

// BTree represents the B-Tree itself
// t is the minimum degree (defines the range for number of keys)
type BTree struct {
	root *BTreeNode
	t    int
}

// NewBTree creates a new B-Tree with given minimum degree
func NewBTree(t int) *BTree {
	return &BTree{root: &BTreeNode{isLeaf: true}, t: t}
}

// Get returns the value and true if key is present in the B-Tree
func (tree *BTree) Get(key string) (string, bool) {
	return tree.root.search(key)
}

func (node *BTreeNode) search(key string) (string, bool) {
	i := 0
	for i < len(node.keys) && key > node.keys[i] {
		i++
	}
	if i < len(node.keys) && key == node.keys[i] && node.values[i] != "<TOMBSTONE!>" {
		return node.values[i], true
	}
	if node.isLeaf {
		return "", false
	}
	return node.children[i].search(key)
}

// Add inserts a key-value pair into the B-Tree
func (tree *BTree) Add(key, value string) {
	root := tree.root
	if len(root.keys) == 2*tree.t-1 {
		s := &BTreeNode{isLeaf: false, children: []*BTreeNode{root}}
		tree.root = s
		s.splitChild(0, tree.t)
		s.addNonFull(key, value, tree.t)
	} else {
		root.addNonFull(key, value, tree.t)
	}
}

func (node *BTreeNode) addNonFull(key, value string, t int) {
	i := len(node.keys) - 1
	if node.isLeaf {
		node.keys = append(node.keys, "")
		node.values = append(node.values, "")
		for i >= 0 && key < node.keys[i] {
			node.keys[i+1] = node.keys[i]
			node.values[i+1] = node.values[i]
			i--
		}
		node.keys[i+1] = key
		node.values[i+1] = value
		return
	}
	for i >= 0 && key < node.keys[i] {
		i--
	}
	i++
	if len(node.children[i].keys) == 2*t-1 {
		node.splitChild(i, t)
		if key > node.keys[i] {
			i++
		}
	}
	node.children[i].addNonFull(key, value, t)
}

func (node *BTreeNode) splitChild(i, t int) {
	y := node.children[i]
	z := &BTreeNode{isLeaf: y.isLeaf}
	z.keys = append(z.keys, y.keys[t:]...)
	z.values = append(z.values, y.values[t:]...)
	y.keys = y.keys[:t-1]
	y.values = y.values[:t-1]
	if !y.isLeaf {
		z.children = append(z.children, y.children[t:]...)
		y.children = y.children[:t]
	}
	node.children = append(node.children, nil)
	copy(node.children[i+2:], node.children[i+1:])
	node.children[i+1] = z
	node.keys = append(node.keys, "")
	node.values = append(node.values, "")
	copy(node.keys[i+1:], node.keys[i:])
	copy(node.values[i+1:], node.values[i:])
	node.keys[i] = y.keys[t-1]
	node.values[i] = y.values[t-1]
}

// Remove sets the value of the key to "<TOMBSTONE!>" if found (logical removal)
func (node *BTreeNode) Remove(key string) {
	i := 0
	for i < len(node.keys) && key > node.keys[i] {
		i++
	}
	if i < len(node.keys) && key == node.keys[i] {
		node.values[i] = "<TOMBSTONE!>"
		return
	}
	if node.isLeaf {
		return
	}
	node.children[i].Remove(key)
}
