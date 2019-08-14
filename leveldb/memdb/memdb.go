// Copyright (c) 2012, Suryandaru Triandana <syndtr@gmail.com>
// All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Package memdb provides in-memory key/value database implementation.
package memdb

import (
	"math/rand"
	"sync"

	"github.com/pingcap/goleveldb/leveldb/comparer"
	"github.com/pingcap/goleveldb/leveldb/errors"
	"github.com/pingcap/goleveldb/leveldb/iterator"
	"github.com/pingcap/goleveldb/leveldb/util"
)

type lockedSource struct {
	lk  sync.Mutex
	src rand.Source
}

func (r *lockedSource) Int63() (n int64) {
	r.lk.Lock()
	n = r.src.Int63()
	r.lk.Unlock()
	return
}

func (r *lockedSource) Seed(seed int64) {
	r.lk.Lock()
	r.src.Seed(seed)
	r.lk.Unlock()
}

// Common errors.
var (
	ErrNotFound     = errors.ErrNotFound
	ErrIterReleased = errors.New("leveldb/memdb: iterator released")
	rndSrc          = &lockedSource{src: rand.NewSource(0xdeadbeef)}
)

const tMaxHeight = 12

type dbIter struct {
	util.BasicReleaser
	p          *DB
	slice      *util.Range
	node       int
	forward    bool
	key, value []byte
	err        error
}

func (i *dbIter) fill(checkStart, checkLimit bool) bool {
	if i.node != 0 {
		n := i.p.nodeData.Get(i.node)
		m := n + i.p.nodeData.Get(i.node+nKey)
		i.key = i.p.kvData.Slice(n, m)
		if i.slice != nil {
			switch {
			case checkLimit && i.slice.Limit != nil && i.p.cmp.Compare(i.key, i.slice.Limit) >= 0:
				fallthrough
			case checkStart && i.slice.Start != nil && i.p.cmp.Compare(i.key, i.slice.Start) < 0:
				i.node = 0
				goto bail
			}
		}
		i.value = i.p.kvData.Slice(m, m+i.p.nodeData.Get(i.node+nVal))
		return true
	}
bail:
	i.key = nil
	i.value = nil
	return false
}

func (i *dbIter) Valid() bool {
	return i.node != 0
}

func (i *dbIter) First() bool {
	if i.Released() {
		i.err = ErrIterReleased
		return false
	}

	i.forward = true
	i.p.mu.RLock()
	defer i.p.mu.RUnlock()
	if i.slice != nil && i.slice.Start != nil {
		i.node, _ = i.p.findGE(i.slice.Start, false)
	} else {
		i.node = i.p.nodeData.Get(nNext)
	}
	return i.fill(false, true)
}

func (i *dbIter) Last() bool {
	if i.Released() {
		i.err = ErrIterReleased
		return false
	}

	i.forward = false
	i.p.mu.RLock()
	defer i.p.mu.RUnlock()
	if i.slice != nil && i.slice.Limit != nil {
		i.node = i.p.findLT(i.slice.Limit)
	} else {
		i.node = i.p.findLast()
	}
	return i.fill(true, false)
}

func (i *dbIter) Seek(key []byte) bool {
	if i.Released() {
		i.err = ErrIterReleased
		return false
	}

	i.forward = true
	i.p.mu.RLock()
	defer i.p.mu.RUnlock()
	if i.slice != nil && i.slice.Start != nil && i.p.cmp.Compare(key, i.slice.Start) < 0 {
		key = i.slice.Start
	}
	i.node, _ = i.p.findGE(key, false)
	return i.fill(false, true)
}

func (i *dbIter) Next() bool {
	if i.Released() {
		i.err = ErrIterReleased
		return false
	}

	if i.node == 0 {
		if !i.forward {
			return i.First()
		}
		return false
	}
	i.forward = true
	i.p.mu.RLock()
	defer i.p.mu.RUnlock()
	i.node = i.p.nodeData.Get(i.node + nNext)
	return i.fill(false, true)
}

func (i *dbIter) Prev() bool {
	if i.Released() {
		i.err = ErrIterReleased
		return false
	}

	if i.node == 0 {
		if i.forward {
			return i.Last()
		}
		return false
	}
	i.forward = false
	i.p.mu.RLock()
	defer i.p.mu.RUnlock()
	i.node = i.p.findLT(i.key)
	return i.fill(true, false)
}

func (i *dbIter) Key() []byte {
	return i.key
}

func (i *dbIter) Value() []byte {
	return i.value
}

func (i *dbIter) Error() error { return i.err }

func (i *dbIter) Release() {
	if !i.Released() {
		i.p = nil
		i.node = 0
		i.key = nil
		i.value = nil
		i.BasicReleaser.Release()
	}
}

const (
	nKV = iota
	nKey
	nVal
	nHeight
	nNext
)

type bitRand struct {
	src   rand.Source
	value uint64
	bits  uint
}

// bitN return a random int value of n bits.
func (r *bitRand) bitN(n uint) uint64 {
	if r.bits < n {
		r.value = uint64(r.src.Int63())
		r.bits = 60
	}

	// take n bits from value
	mask := (1 << n) - 1
	ret := r.value & uint64(mask)

	r.value = r.value >> n
	r.bits -= n
	return ret
}

// DB is an in-memory key/value database.
type DB struct {
	cmp comparer.BasicComparer
	rnd *bitRand

	mu     sync.RWMutex
	kvData byteSlice
	// Node data:
	// [0]         : KV offset
	// [1]         : Key length
	// [2]         : Value length
	// [3]         : Height
	// [3..height] : Next nodes
	nodeData  nodeData
	prevNode  [tMaxHeight]int
	maxHeight int
	n         int
	kvSize    int
}

// randHeight returns a random int value for the height of a skip-list node.
func (p *DB) randHeight() (h int) {
	h = 1
	// From wikipedia: https://en.wikipedia.org/wiki/Skip_list
	// "Each higher layer acts as an "express lane" for the lists below, where
	// an element in layer i appears in layer i+1 with some fixed probability p
	// (two commonly used values for p are 1/2 or 1/4)."

	// here we chose 1/4 as the probability, which means
	// 1/4 possibility return a height of 2,
	// 1/16 possibility of height 3,
	// 1/64 possibility of height 4, and so on...
	for h < tMaxHeight && p.rnd.bitN(2) == 0 {
		h++
	}
	return
}

// Must hold RW-lock if prev == true, as it use shared prevNode slice.
func (p *DB) findGE(key []byte, prev bool) (int, bool) {
	node := 0
	h := p.maxHeight - 1
	for {
		next := p.nodeData.Get(node + nNext + h)
		cmp := 1
		if next != 0 {
			o := p.nodeData.Get(next)
			cmp = p.cmp.Compare(p.kvData.Slice(o, o+p.nodeData.Get(next+nKey)), key)
		}
		if cmp < 0 {
			// Keep searching in this list
			node = next
		} else {
			if prev {
				p.prevNode[h] = node
			} else if cmp == 0 {
				return next, true
			}
			if h == 0 {
				return next, cmp == 0
			}
			h--
		}
	}
}

func (p *DB) findLT(key []byte) int {
	node := 0
	h := p.maxHeight - 1
	for {
		next := p.nodeData.Get(node + nNext + h)
		o := p.nodeData.Get(next)
		if next == 0 || p.cmp.Compare(p.kvData.Slice(o, o+p.nodeData.Get(next+nKey)), key) >= 0 {
			if h == 0 {
				break
			}
			h--
		} else {
			node = next
		}
	}
	return node
}

func (p *DB) findLast() int {
	node := 0
	h := p.maxHeight - 1
	for {
		next := p.nodeData.Get(node + nNext + h)
		if next == 0 {
			if h == 0 {
				break
			}
			h--
		} else {
			node = next
		}
	}
	return node
}

// Put sets the value for the given key. It overwrites any previous value
// for that key; a DB is not a multi-map.
//
// It is safe to modify the contents of the arguments after Put returns.
func (p *DB) Put(key []byte, value []byte) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if node, exact := p.findGE(key, true); exact {
		kvOffset := p.kvData.Allocate(len(key) + len(value))
		p.kvData.Append(key)
		p.kvData.Append(value)
		p.nodeData.Set(node, kvOffset)
		m := p.nodeData.Get(node + nVal)
		p.nodeData.Set(node+nVal, len(value))
		p.kvSize += len(value) - m
		return nil
	}

	h := p.randHeight()
	if h > p.maxHeight {
		for i := p.maxHeight; i < h; i++ {
			p.prevNode[i] = 0
		}
		p.maxHeight = h
	}

	kvOffset := p.kvData.Allocate(len(key) + len(value))
	p.kvData.Append(key)
	p.kvData.Append(value)
	// Node
	node := p.nodeData.Allocate(4 + h)
	p.nodeData.Set(node, kvOffset)
	p.nodeData.Set(node+1, len(key))
	p.nodeData.Set(node+2, len(value))
	p.nodeData.Set(node+3, h)
	for i, n := range p.prevNode[:h] {
		m := n + nNext + i
		p.nodeData.Set(node+4+i, p.nodeData.Get(m))
		p.nodeData.Set(m, node)
	}

	p.kvSize += len(key) + len(value)
	p.n++
	return nil
}

// Delete deletes the value for the given key. It returns ErrNotFound if
// the DB does not contain the key.
//
// It is safe to modify the contents of the arguments after Delete returns.
func (p *DB) Delete(key []byte) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	node, exact := p.findGE(key, true)
	if !exact {
		return ErrNotFound
	}

	h := p.nodeData.Get(node + nHeight)
	for i, n := range p.prevNode[:h] {
		m := n + 4 + i
		p.nodeData.Set(m, p.nodeData.Get(p.nodeData.Get(m)+nNext+i))
	}

	p.kvSize -= p.nodeData.Get(node+nKey) + p.nodeData.Get(node+nVal)
	p.n--
	return nil
}

// Contains returns true if the given key are in the DB.
//
// It is safe to modify the contents of the arguments after Contains returns.
func (p *DB) Contains(key []byte) bool {
	p.mu.RLock()
	_, exact := p.findGE(key, false)
	p.mu.RUnlock()
	return exact
}

// Get gets the value for the given key. It returns error.ErrNotFound if the
// DB does not contain the key.
//
// The caller should not modify the contents of the returned slice, but
// it is safe to modify the contents of the argument after Get returns.
func (p *DB) Get(key []byte) (value []byte, err error) {
	p.mu.RLock()
	if node, exact := p.findGE(key, false); exact {
		o := p.nodeData.Get(node) + p.nodeData.Get(node+nKey)
		value = p.kvData.Slice(o, o+p.nodeData.Get(node+nVal))
	} else {
		err = ErrNotFound
	}
	p.mu.RUnlock()
	return
}

// Find finds key/value pair whose key is greater than or equal to the
// given key. It returns ErrNotFound if the table doesn't contain
// such pair.
//
// The caller should not modify the contents of the returned slice, but
// it is safe to modify the contents of the argument after Find returns.
func (p *DB) Find(key []byte) (rkey, value []byte, err error) {
	p.mu.RLock()
	if node, _ := p.findGE(key, false); node != 0 {
		n := p.nodeData.Get(node)
		m := n + p.nodeData.Get(node+nKey)
		rkey = p.kvData.Slice(n, m)
		value = p.kvData.Slice(m, m+p.nodeData.Get(node+nVal))
	} else {
		err = ErrNotFound
	}
	p.mu.RUnlock()
	return
}

// NewIterator returns an iterator of the DB.
// The returned iterator is not safe for concurrent use, but it is safe to use
// multiple iterators concurrently, with each in a dedicated goroutine.
// It is also safe to use an iterator concurrently with modifying its
// underlying DB. However, the resultant key/value pairs are not guaranteed
// to be a consistent snapshot of the DB at a particular point in time.
//
// Slice allows slicing the iterator to only contains keys in the given
// range. A nil Range.Start is treated as a key before all keys in the
// DB. And a nil Range.Limit is treated as a key after all keys in
// the DB.
//
// The iterator must be released after use, by calling Release method.
//
// Also read Iterator documentation of the leveldb/iterator package.
func (p *DB) NewIterator(slice *util.Range) iterator.Iterator {
	return &dbIter{p: p, slice: slice}
}

// Capacity returns keys/values buffer capacity.
func (p *DB) Capacity() int {
	p.mu.RLock()
	defer p.mu.RUnlock()
	last := p.kvData.chunks
	// TODO:
	return cap(last)
}

// Size returns sum of keys and values length. Note that deleted
// key/value will not be accounted for, but it will still consume
// the buffer, since the buffer is append only.
func (p *DB) Size() int {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.kvSize
}

// Free returns keys/values free buffer before need to grow.
func (p *DB) Free() int {
	p.mu.RLock()
	defer p.mu.RUnlock()
	last := p.kvData.chunks
	return cap(last) - len(last)
}

// Len returns the number of entries in the DB.
func (p *DB) Len() int {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.n
}

// Reset resets the DB to initial empty state. Allows reuse the buffer.
func (p *DB) Reset() {
	p.mu.Lock()
	p.maxHeight = 1
	p.n = 0
	p.kvSize = 0
	p.kvData.Truncate(0)
	p.nodeData.Truncate(nNext + tMaxHeight)
	p.nodeData.Set(nKV, 0)
	p.nodeData.Set(nKey, 0)
	p.nodeData.Set(nVal, 0)
	p.nodeData.Set(nHeight, tMaxHeight)
	for n := 0; n < tMaxHeight; n++ {
		p.prevNode[n] = 0
	}
	p.mu.Unlock()
}

// New creates a new initialized in-memory key/value DB. The capacity
// is the initial key/value buffer capacity. The capacity is advisory,
// not enforced.
//
// This DB is append-only, deleting an entry would remove entry node but not
// reclaim KV buffer.
//
// The returned DB instance is safe for concurrent use.
func New(cmp comparer.BasicComparer, capacity int) *DB {
	p := &DB{
		cmp:       cmp,
		rnd:       &bitRand{src: rndSrc},
		kvData:    newByteSlice(capacity),
		nodeData:  newNodeData(4+tMaxHeight, 1024),
		maxHeight: 1,
	}
	p.nodeData.Set(nHeight, tMaxHeight)
	return p
}
