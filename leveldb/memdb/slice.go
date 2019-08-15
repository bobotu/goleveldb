package memdb

import "math/bits"

const (
	byteOffsetShift  = 21
	byteMaxChunkSize = 1 << byteOffsetShift
)

type byteSlice struct {
	chunks []byte
}

func newByteSlice(initCap int) byteSlice {
	//initCap = round(initCap)
	return byteSlice{chunks: make([]byte, 0, initCap)}
}

func (s *byteSlice) Allocate(size int) int {
	return len(s.chunks)
}

func (s *byteSlice) Truncate(end int) {
	s.chunks = s.chunks[:end]
}

func (s *byteSlice) Append(data []byte) {
	s.chunks = append(s.chunks, data...)
}

func (s *byteSlice) Slice(start, end int) []byte {
	return s.chunks[start:end]
}

func (s *byteSlice) decodeIdx(idx int) (int, int) {
	return idx >> byteOffsetShift, idx & (1<<byteOffsetShift - 1)
}

type nodeData struct {
	chunkSize   int
	offShift    uint
	chunksSmall [10][]int
	chunks      [][]int
}

func newNodeData(initSize, chunkSize int) nodeData {
	chunkSize = round(chunkSize)
	d := nodeData{
		chunkSize: chunkSize,
		offShift:  uint(bits.TrailingZeros64(uint64(chunkSize))),
	}
	d.chunksSmall[0] = make([]int, initSize, chunkSize)
	d.chunks = d.chunksSmall[:1]
	return d
}

func (d *nodeData) Allocate(size int) int {
	idx := len(d.chunks) - 1
	off := len(d.chunks[idx])
	remain := cap(d.chunks[idx]) - off
	if size > remain {
		d.chunks = append(d.chunks, make([]int, size-remain, d.chunkSize))
		size = remain
	}
	d.chunks[idx] = d.chunks[idx][:off+size]
	return idx<<d.offShift + off
}

func (d *nodeData) Truncate(end int) {
	c, off := d.decodeIdx(end)
	d.chunks[c] = d.chunks[c][:off]
	d.chunks = d.chunks[:c+1]
}

func (d *nodeData) Get(idx int) int {
	c, off := d.decodeIdx(idx)
	chunk := d.chunks[c]
	return chunk[off]
}

func (d *nodeData) Set(idx int, value int) {
	c, off := d.decodeIdx(idx)
	d.chunks[c][off] = value
}

func (d *nodeData) decodeIdx(idx int) (int, int) {
	return idx >> d.offShift, idx & (1<<d.offShift - 1)
}

// Round up to the next highest power of 2
func round(a int) int {
	a--
	a |= a >> 1
	a |= a >> 2
	a |= a >> 4
	a |= a >> 8
	a |= a >> 16
	return a + 1
}
