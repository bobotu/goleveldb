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
	chunkSize int
	offShift  uint
	chunks    [][]int
}

func newNodeData(chunkSize int) nodeData {
	chunkSize = round(chunkSize)
	return nodeData{
		chunkSize: chunkSize,
		offShift:  uint(bits.TrailingZeros64(uint64(chunkSize))),
		chunks:    [][]int{make([]int, 0, chunkSize)},
	}
}

func (d *nodeData) CurrentPosition() int {
	idx := len(d.chunks) - 1
	off := len(d.chunks[idx])
	return idx<<d.offShift + off
}

func (d *nodeData) Truncate(end int) {
	c, off := d.decodeIdx(end)
	d.chunks[c] = d.chunks[c][:off]
	d.chunks = d.chunks[:c+1]
}

func (d *nodeData) Append(data []int) {
	for {
		chunk := d.chunks[len(d.chunks)-1]
		remain := cap(chunk) - len(chunk)
		cursor := len(data)
		if remain < len(data) {
			cursor = remain
		}
		d.chunks[len(d.chunks)-1] = append(d.chunks[len(d.chunks)-1], data[:cursor]...)
		data = data[cursor:]
		if len(data) == 0 {
			break
		}
		d.chunks = append(d.chunks, make([]int, 0, d.chunkSize))
	}
}

func (d *nodeData) Get(idx int) int {
	c, off := d.decodeIdx(idx)
	return d.chunks[c][off]
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
