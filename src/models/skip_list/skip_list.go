package skiplist

import (
	"fmt"
	"math/rand"
	"time"
)

type SkipList struct {
	head   *Node
	levels int
}

func (list *SkipList) initialize() {
	list.head = &Node{key: ""}
	tmp := list.head
	for i := 0; i < list.levels; i++ {
		tmp.below = &Node{key: ""}
		tmp = tmp.below
	}
}

func (list *SkipList) Get(key string) (string, bool) {
	tmp := list.head

	for {
		fmt.Println(tmp)
		if tmp.key == key {
			if tmp.isDeleted {
				return "", false
			}
			return tmp.value, true
		}
		if tmp.right == nil {
			if tmp.below == nil {
				return "", false
			} else {
				tmp = tmp.below
			}
		} else {
			if tmp.right.key > key {
				return "", false
			}
			tmp = tmp.right
		}
	}
}

func (list *SkipList) Remove(key string) bool {
	node, exists := list.findToRemove(key)

	if !exists {
		return false
	}

	for node != nil {
		node.isDeleted = true
		node = node.below
	}
	return true
}

func (list *SkipList) findToRemove(key string) (*Node, bool) {
	tmp := list.head

	for {
		if tmp.right != nil && tmp.right.key == key {
			return tmp.right, true
		}
		if tmp.right == nil && tmp.below == nil {
			return nil, false
		}

		if tmp.right == nil || tmp.right.key > key {
			tmp = tmp.below
		} else {
			tmp = tmp.right
		}
	}
}

func (list *SkipList) findToAdd(key string) []*Node {
	node := list.head
	lefts := make([]*Node, 0, list.levels)

	for {
		if node.right == nil && node.below == nil {
			return append(lefts, node)
		}
		if node.right != nil && node.right.key > key && node.below == nil {
			return append(lefts, node)
		}

		if node.right == nil || node.right.key > key {
			lefts = append(lefts, node)
			node = node.below
		} else {
			node = node.right
		}
	}
}

func coinFlip(r *rand.Rand) string {
	flip := r.Intn(2)
	if flip == 0 {
		return "Heads"
	}
	return "Tails"
}

func (list *SkipList) Add(key string, value string) {
	if list.head == nil {
		list.initialize()
	}
	times_to_add := 1

	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	for ; times_to_add < list.levels; times_to_add++ {
		if coinFlip(r) == "Tails" {
			break
		}
	}

	lefts := list.findToAdd(key)

	added := make([]*Node, times_to_add)
	for i := len(lefts) - 1; i >= 0; i-- {
		if times_to_add == 0 {
			break
		}
		node := &Node{key: key, value: value, isDeleted: false}
		tmp := lefts[i].right
		node.right = tmp
		lefts[i].right = node
		added[times_to_add-1] = node
		times_to_add--
	}

	for i := 0; i < len(added)-1; i++ {
		added[i].below = added[i+1]
	}
}

func (list *SkipList) Print() {
	node := list.head
	for i := 0; i < list.levels+1; i++ {
		fmt.Println(i + 1)
		nextLevel := node.below
		for {
			fmt.Print(node)
			if node.right == nil {
				break
			} else {
				node = node.right
			}
		}
		fmt.Println()
		node = nextLevel
	}
}
