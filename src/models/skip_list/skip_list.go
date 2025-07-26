package skiplist

import (
	"encoding/gob"
	"fmt"
	"math/rand"
	"nosqlEngine/src/config"
	"os"
	"path/filepath"
	"time"
)

type Node struct {
	Key   string
	Value string
	Right *Node
	Below *Node
}

type SkipList struct {
	Head   *Node
	Levels int
}

var CONFIG = config.GetConfig()

func (list *SkipList) initialize() {
	list.Head = &Node{Key: ""}
	tmp := list.Head
	for i := 0; i < list.Levels; i++ {
		tmp.Below = &Node{Key: ""}
		tmp = tmp.Below
	}
}

func NewSkipList(levels int) *SkipList {
	if levels < 1 {
		levels = 1
	}
	list := &SkipList{Levels: levels}
	list.initialize()
	return list
}

func (list *SkipList) Get(key string) (string, bool) {
	tmp := list.Head

	for {
		if tmp.Key == key {
			if tmp.Value == CONFIG.Tombstone {
				return "", false
			}
			return tmp.Value, true
		}
		if tmp.Right == nil {
			if tmp.Below == nil {
				return "", false
			}
			tmp = tmp.Below
		} else {
			if tmp.Right.Key > key {
				return "", false
			}
			tmp = tmp.Right
		}
	}
}

func (list *SkipList) Remove(key string) bool {
	node, exists := list.findToRemove(key)
	if !exists {
		return false
	}
	for node != nil {
		node.Value = CONFIG.Tombstone
		node = node.Below
	}
	return true
}

func (list *SkipList) findToRemove(key string) (*Node, bool) {
	tmp := list.Head

	for {
		if tmp.Right != nil && tmp.Right.Key == key {
			return tmp.Right, true
		}
		if tmp.Right == nil && tmp.Below == nil {
			return nil, false
		}

		if tmp.Right == nil || tmp.Right.Key > key {
			tmp = tmp.Below
		} else {
			tmp = tmp.Right
		}
	}
}

func (list *SkipList) findToAdd(key string) []*Node {
	node := list.Head
	lefts := make([]*Node, 0, list.Levels)

	for {
		if node.Right == nil && node.Below == nil {
			return append(lefts, node)
		}
		if node.Right != nil && node.Right.Key > key && node.Below == nil {
			return append(lefts, node)
		}

		if node.Right == nil || node.Right.Key > key {
			lefts = append(lefts, node)
			node = node.Below
		} else {
			node = node.Right
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

func (list *SkipList) Add(key string, value string) bool {
	if list.Head == nil {
		list.initialize()
	}
	times_to_add := 1

	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	for ; times_to_add < list.Levels; times_to_add++ {
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
		node := &Node{Key: key, Value: value}
		tmp := lefts[i].Right
		node.Right = tmp
		lefts[i].Right = node
		added[times_to_add-1] = node
		times_to_add--
	}

	for i := 0; i < len(added)-1; i++ {
		added[i].Below = added[i+1]
	}
	return true
}

func (list *SkipList) Print() {
	node := list.Head
	for i := 0; i < list.Levels+1; i++ {
		fmt.Printf("Level %d: ", i+1)
		nextLevel := node.Below
		tmp := node
		for tmp != nil {
			if tmp.Key != "" {
				fmt.Printf("(%s,%s) ", tmp.Key, tmp.Value)
			}
			tmp = tmp.Right
		}
		fmt.Println()
		node = nextLevel
	}
}

func (list *SkipList) Serialize(filename string) error {
	path := filepath.Join("src/models/serialized", filename)
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()
	enc := gob.NewEncoder(file)
	return enc.Encode(list)
}

func Deserialize(filename string) (*SkipList, error) {
	path := filepath.Join("src/models/serialized", filename)
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	dec := gob.NewDecoder(file)
	var list SkipList
	if err := dec.Decode(&list); err != nil {
		return nil, err
	}
	return &list, nil
}
