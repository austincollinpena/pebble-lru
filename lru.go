package lruCache

import (
	"bytes"
	"container/list"
	"errors"
	"fmt"
	"github.com/cockroachdb/pebble"
	"log"
	"sync"
)

// https://github.com/hashicorp/golang-lru/blob/master/simplelru/lru.go
// https://github.com/golang/groupcache/blob/master/lru/lru.go
// https://github.com/hashicorp/golang-lru/blob/master/lru.go

type Cache struct {
	// The amount of disk space you're willing to let this take up
	MaxSizeInBytes uint64
	// Counts the number of sets. Used to only call file.Stat() every certain number of sets
	hitCounter int
	// Clears this many items from the cache. Gives time for cache to regrow and then
	// re-shrink aggressively, rather than deleting on every set
	RemoveThisManyItemsFromTheCache int
	// Location of the DB
	FileLocation string
	// To move an accessed element to the front, this stores the actual list element
	// Therefore, when we look for it, we're calling the actual element
	// The string key is the same key for the list element AND the key set into the
	// bbolt database
	listOfEvictList map[string]*list.Element
	// All of the items set through the interface in this package.
	// They are all available for eviction at any time.
	evictList *list.List
	lock      sync.RWMutex
}

// Creates a new cache with a specified byte size
func NewCache(maxSizeInBytes uint64, removeThisManyItemsFromTheCache int, fileLocation string) (*Cache, error) {
	c := &Cache{
		MaxSizeInBytes:                  maxSizeInBytes,
		hitCounter:                      0,
		RemoveThisManyItemsFromTheCache: removeThisManyItemsFromTheCache,
		FileLocation:                    fileLocation,
		listOfEvictList:                 make(map[string]*list.Element),
		evictList:                       list.New(),
		lock:                            sync.RWMutex{},
	}
	return c, nil
}

var nilError = errors.New("nil")

// Retrieves the item
// If it exists, pushes the item to the front of the cache
func (c *Cache) Get(key []byte, db *pebble.DB) (value []byte, err error) {
	val, closer, err := db.Get(key)
	if err != nil {
		return nil, err
	}
	if err := closer.Close(); err != nil {
		return nil, err
	}
	if val == nil {
		return nil, nilError
	}
	c.lock.Lock()
	c.moveItemToFront(key)
	c.lock.Unlock()
	return val, nil
}

// Public Interface for setting cache item
// Recalculates the average size of the documents
// Evicts if the size of the cache is too large
// Updates the current size of the cache
// Adds the key and value to the bolt bucket
// Adds the key to the list of keys
// Adds the key to evict list
func (c *Cache) Set(key []byte, val []byte, db *pebble.DB) error {
	c.lock.Lock()
	c.moveItemToFront(key)
	c.hitCounter = c.hitCounter + 1
	c.lock.Unlock()
	// Manage clearing the cache only once every 20 sets
	// Adjust this number back to 20 after debugging for better
	// performance
	if c.hitCounter%1 == 0 {
		tableSize := getTableSize(db)
		if tableSize > c.MaxSizeInBytes {
			err := c.evictFromCache(db)
			if err != nil {
				log.Fatal(err)
			}
		}
	}
	err := db.Set(key, val, pebble.Sync)
	if err != nil {
		return err
	}
	return nil
}

func getTableSize(db *pebble.DB) uint64 {
	tables := db.SSTables()
	var size uint64
	for _, table := range tables {
		for _, info := range table {
			size = size + info.Size
		}
	}
	return size
}

// Used to add to the list, as the list takes in a raw interface
type listEl struct {
	val interface{}
}

// After access, the item is retrieved from the "list of the list" if it exists
// And added to the front
// If it doesn't exist yet, it is added to the "list of the list" and pushed to the
// front
func (c *Cache) moveItemToFront(val []byte) {
	// Move an existing key to the front
	if ent, ok := c.listOfEvictList[string(val)]; ok {
		c.evictList.MoveToFront(ent)
		//fmt.Println("debug spot")
	} else {
		ent := listEl{val: val}
		newEntry := c.evictList.PushFront(ent)
		c.listOfEvictList[string(val)] = newEntry
		//fmt.Println("debug spot")
	}
}

// Used for bulk removing keys
func (c *Cache) RemoveSpecificItemsFromCache(db *pebble.DB, items [][]byte) error {
	panic("oof")
}

// Removes from the evict list
// Removes from the list of the evict list
// Removes from the bolt bucket
// WARNING: If this is called for an existing database, this will not work.
// This cache only works for fresh databases because
func (c *Cache) evictFromCache(db *pebble.DB) error {
	c.lock.Lock()
	valuesRemoved := 0
	valsToRemove := [][]byte{}
	elementsToRemove := []*list.Element{}
	for e := c.evictList.Back(); e != nil; e = e.Prev() {
		// Item Value is stored as a listEl.
		//The val in there (an interface) is a byte array
		val := e.Value.(listEl).val.([]byte)
		valsToRemove = append(valsToRemove, val)
		elementsToRemove = append(elementsToRemove, e)
		// Alerts if a key is being deleted
		if bytes.Equal(val, []byte("a")) {
			fmt.Println("Deleting A")
		}
		delete(c.listOfEvictList, string(val))
		valuesRemoved += 1
		if valuesRemoved >= c.RemoveThisManyItemsFromTheCache {
			break
		}
	}
	for _, val := range elementsToRemove {
		c.evictList.Remove(val)
	}
	c.lock.Unlock()
	for _, val := range valsToRemove {
		err := db.Delete(val, pebble.Sync)
		if err != nil {
			log.Fatal(err)
		}
	}
	return nil
}
