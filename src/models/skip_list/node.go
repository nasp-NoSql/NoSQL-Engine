package skiplist

type Node struct {
	key       string
	value     string
	right     *Node
	below     *Node
	isDeleted bool
}
