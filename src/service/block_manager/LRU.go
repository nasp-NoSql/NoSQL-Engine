package block_manager

import (
	"fmt"
	doublyll "nosqlEngine/src/models/doubly_ll"
)

type LRUCache struct {
	capacity int
	cache    map[string]*doublyll.Block
	lruList  *doublyll.DoublyLinkedList
	evicted  *EvictedBlock
}

type EvictedBlock struct {
	filePath string
	blockID  int
	data     []byte
}

func NewLRUCache(capacity int) *LRUCache {
	return &LRUCache{
		capacity: capacity,
		cache:    make(map[string]*doublyll.Block),
		lruList:  doublyll.NewDoublyLinkedList(),
		evicted:  nil,
	}
}

func (c *LRUCache) Put(filePath string, blockID int, data []byte) error {
	key := fmt.Sprintf("%s:%d", filePath, blockID)

	if elem, found := c.cache[key]; found {
		c.lruList.MoveToFront(elem)
		elem.Set(data)
		return nil
	}

	newBlock := doublyll.NewNode(data, blockID, filePath)
	c.lruList.InsertBeginning(newBlock)
	c.cache[key] = newBlock

	if c.lruList.ListLength() > c.capacity {
		c.evict()
	}
	return nil
}

func (c *LRUCache) evict() {
	elem := c.lruList.Back()
	if elem == nil {
		return
	}

	block := elem
	c.evicted = &EvictedBlock{block.GetFilename(), block.GetNumber(), block.Get()}
	delete(c.cache, fmt.Sprintf("%s:%d", c.evicted.filePath, c.evicted.blockID))
	c.lruList.DeleteEnd()
}

func (c *LRUCache) Get(filePath string, blockID int) ([]byte, error) {
	key := fmt.Sprintf("%s:%d", filePath, blockID)
	if elem, found := c.cache[key]; found {
		c.lruList.MoveToFront(elem)
		return elem.Get(), nil
	}
	return nil, fmt.Errorf("Block not found")
}

func (c *LRUCache) GetEvictedBlock() (string, int, []byte) {
	if c.evicted == nil {
		return "", 0, nil
	}
	filePath, blockID, data := c.evicted.filePath, c.evicted.blockID, c.evicted.data
	c.evicted = nil
	return filePath, blockID, data
}
