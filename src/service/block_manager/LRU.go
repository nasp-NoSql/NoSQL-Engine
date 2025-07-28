package block_manager

import (
	"fmt"
	cfg "nosqlEngine/src/config"
	doublyll "nosqlEngine/src/models/doubly_ll"
)

type LRUCache struct {
	capacity int
	cache    map[doublyll.BlockKey]*doublyll.Block
	lruList  *doublyll.DoublyLinkedList
	evicted  *EvictedBlock
}

type EvictedBlock struct {
	blockKey doublyll.BlockKey
	data     []byte
}

func NewLRUCache() *LRUCache {
	capacity := cfg.GetConfig().CacheCapacity
	return &LRUCache{
		capacity: capacity,
		cache:    make(map[doublyll.BlockKey]*doublyll.Block),
		lruList:  doublyll.NewDoublyLinkedList(),
		evicted:  nil,
	}
}

func (c *LRUCache) Put(filePath string, blockID int, data []byte) error {
	key := doublyll.NewBlockKey(blockID, filePath)
	if elem, found := c.cache[key]; found {
		c.lruList.MoveToFront(elem)
		elem.Set(data)
		return nil
	}

	newBlock := doublyll.NewNode(data, key)
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
	c.evicted = &EvictedBlock{block.BlockKey, block.Get()}
	delete(c.cache, block.BlockKey)
	c.lruList.DeleteEnd()
}

func (c *LRUCache) Get(filePath string, blockID int) ([]byte, error) {
	key := doublyll.NewBlockKey(blockID, filePath)
	fmt.Print("Fetching block from cache:", key)
	if elem, found := c.cache[key]; found {
		c.lruList.MoveToFront(elem)
		return elem.Get(), nil
	}
	return nil, fmt.Errorf("block not found")
}

func (c *LRUCache) GetEvictedBlock() (doublyll.BlockKey, []byte) {
	if c.evicted == nil {
		return 0, nil
	}
	blockKey, data := c.evicted.blockKey, c.evicted.data
	c.evicted = nil
	return blockKey, data
}
