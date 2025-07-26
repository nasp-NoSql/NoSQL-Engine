package skiplist

import (
	"encoding/gob"
	"fmt"
	"math/rand"
	"nosqlEngine/src/config"
	"nosqlEngine/src/models/key_value"
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
	Size   int
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
	
	// Check if it's already a tombstone
	alreadyTombstone := (node.Value == "<TOMBSTONE!>")
	
	for node != nil {
		node.Value = CONFIG.Tombstone
		node = node.Below
	}
	
	// Only decrement size if it wasn't already a tombstone
	if !alreadyTombstone {
		list.Size--
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

	// Check if the key already exists
	_, exists := list.Get(key)
	if exists {
		// Update all nodes with this key
		return list.updateExistingKey(key, value)
	}

	// Key doesn't exist, add new node(s)
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
	
	// Increment size since we added a new key
	list.Size++
	return true
}

// updateExistingKey updates the value for all nodes with the given key
func (list *SkipList) updateExistingKey(key string, value string) bool {
	node := list.Head
	updated := false
	
	// Traverse all levels
	for node != nil {
		tmp := node
		// Traverse horizontally on this level
		for tmp != nil {
			if tmp.Key == key {
				tmp.Value = value
				updated = true
			}
			tmp = tmp.Right
		}
		node = node.Below
	}
	
	return updated
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

func (list *SkipList) ToRaw() []key_value.KeyValue {
	ret := make([]key_value.KeyValue, 0)
	node := list.Head
	for node != nil && node.Below != nil {
		node = node.Below
	}
	for node != nil {
		tmp := node.Right
		for tmp != nil {
			if tmp.Key != "" {
				ret = append(ret, key_value.NewKeyValue(tmp.Key, tmp.Value))
			}
			tmp = tmp.Right
		}
	}
	return ret
}
func (list *SkipList) Clear() bool {
	list.Head = &Node{Key: ""}
	tmp := list.Head
	for i := 0; i < list.Levels; i++ {
		tmp.Below = &Node{Key: ""}
		tmp = tmp.Below
	}
	list.Size = 0
	list.Levels = 1
	return true
}