package lruCache

import (
	"bytes"
	"fmt"
	"github.com/cockroachdb/pebble"
	"io/ioutil"
	"log"
	"os"
	"path"
	"strconv"
	"testing"
)

func TestLruCache(t *testing.T) {
	dir, err := ioutil.ReadDir("./tmp/pebble/demo")
	for _, d := range dir {
		os.RemoveAll(path.Join([]string{"./tmp/pebble/demo", d.Name()}...))
	}
	Pdb, err := pebble.Open("./tmp/pebble/demo", &pebble.Options{})
	if err != nil {
		log.Fatal(err)
	}
	var maxSize uint64 = 2000000
	t.Run("Keep 'hot' items in the cache across cache evictions", func(t *testing.T) {
		lru, err := NewCache(2000000,
			1,
			"./tmp/pebble/demo") //8k KB
		if err != nil {
			t.Fail()
			panic(err)
		}
		type arr struct {
			key []byte
			val []byte
		}
		testCases := []arr{}
		for i := 0; i < 10000; i++ {
			testCases = append(testCases, arr{
				key: []byte(strconv.Itoa(i)),
				val: getBytes(),
			})
		}
		lru.Set([]byte("a"), []byte("2"), Pdb)
		for _, val := range testCases {
			err := lru.Set(val.key, val.val, Pdb)
			if err != nil {
				log.Fatal(err)
			}
			newVal, err := lru.Get([]byte("a"), Pdb)
			if !bytes.Equal(newVal, []byte("2")) {
				t.Fail()
			}
			fmt.Println(string(newVal))
			if err != nil {
				log.Fatal(err)
			}
		}
		val, err := lru.Get([]byte("a"), Pdb)
		if err != nil {
			t.Fail()
		}
		if bytes.Equal(val, []byte("2")) {
			fmt.Println("Holy shit!")
		} else {
			t.Fail()
		}
	})
	t.Run("Get the size of the table", func(t *testing.T) {
		size := getTableSize(Pdb)
		if size > maxSize {
			t.Fail()
		}
		fmt.Println("Final size of table is", size)
	})
}

// No cache clearing
// 37665337300ns or 37.6 seconds

// Benchmark for 8000000 byte cache
// Remove 100 & Delete = delete            | 24132158200 ns or 24.1
// Remove 100 & Delete = singleDelete      | 21961556900 ns or 21.9
// Remove 100 & Delete = batch             | 38544006800 ns or 38.5

// Remove 10 & Delete = singleDelete       | 95704072800 ns or 97.7

// Remove 100 & check cache size every 90  | 27555518000 ns or 27
// Remove 100 & check cache size every 1   | 39158374000 ns or 39.1
// Remove 100 & check cache size every 50  | 26180059800 ns or 26.1
// Remove 100 & check cache size every 20  | 21565817600 ns or 21.5

func BenchmarkLRU(b *testing.B) {
	Pdb, err := pebble.Open("./tmp/pebble/demo", &pebble.Options{})
	lru, err := NewCache(8000000,
		1000,
		"./tmp/pebble/demo") //8k KB
	if err != nil {
		panic(err)
	}
	type arr struct {
		key []byte
		val []byte
	}
	testCases := []arr{}
	for i := 0; i < 10000; i++ {
		testCases = append(testCases, arr{
			key: []byte(strconv.Itoa(i)),
			val: getBytes(),
		})
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, val := range testCases {
			err := lru.Set(val.key, val.val, Pdb)
			if err != nil {
				log.Fatal(err)
			}
		}
	}
}
