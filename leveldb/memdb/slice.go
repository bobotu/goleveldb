package memdb

import "unsafe"

const (
	byteChunkSize = 2 * 1024 * 1024
	intChunkSize  = byteChunkSize / unsafe.Sizeof(int(0))
)

type byteChunk struct {
	startOffset int
	used        int
	data        [byteChunkSize]byte
}

func (c *byteChunk) remain() int {
	return len(c.data) - c.used
}

func (c *byteChunk) endOffset() int {
	return c.startOffset + c.used
}

type byteSlice struct {
	chunks []*byteChunk
}

func (s *byteSlice) Len() int {
	if len(s.chunks) == 0 {
		return 0
	}
	lastChunk := s.lastChunk()
	return lastChunk.startOffset + lastChunk.used
}

func (s *byteSlice) Truncate(end int) {
	c, idx := s.findChunk(end)
	c.used = end - c.startOffset
	s.chunks = s.chunks[:idx+1]
}

func (s *byteSlice) Append(data []byte) {
	if len(s.chunks) == 0 {
		s.chunks = append(s.chunks, new(byteChunk))
	}
	lastChunk := s.lastChunk()
	if lastChunk.remain() < len(data) {
		newChunk := new(byteChunk)
		newChunk.startOffset = lastChunk.startOffset + lastChunk.used
		s.chunks = append(s.chunks, newChunk)
		lastChunk = newChunk
	}
	copy(lastChunk.data[lastChunk.used:], data)
	lastChunk.used += len(data)
}

func (s *byteSlice) Slice(start, end int) []byte {
	c, _ := s.findChunk(end - 1)
	if start < c.startOffset {
		panic("slice across chunk")
	}
	start -= c.startOffset
	end -= c.startOffset
	return c.data[start:end]
}

func (s *byteSlice) lastChunk() *byteChunk {
	return s.chunks[len(s.chunks)-1]
}

func (s *byteSlice) findChunk(end int) (*byteChunk, int) {
	idx := end / byteChunkSize
	for s.chunks[idx].endOffset() < end {
		idx++
	}
	return s.chunks[idx], idx
}

type intChunk struct {
	startOffset int
	used        int
	data        [intChunkSize]int
}

func (c *intChunk) remain() int {
	return len(c.data) - c.used
}

func (c *intChunk) endOffset() int {
	return c.startOffset + c.used - 1
}

type intSlice struct {
	chunks []*intChunk
}

func (s *intSlice) Len() int {
	if len(s.chunks) == 0 {
		return 0
	}
	lastChunk := s.lastChunk()
	return lastChunk.startOffset + lastChunk.used
}

func (s *intSlice) Truncate(end int) {
	c, idx := s.findChunk(end)
	c.used = end - c.startOffset
	s.chunks = s.chunks[:idx+1]
}

func (s *intSlice) Append(data []int) {
	if len(s.chunks) == 0 {
		s.chunks = append(s.chunks, new(intChunk))
	}
	lastChunk := s.lastChunk()
	if lastChunk.remain() < len(data) {
		newChunk := new(intChunk)
		newChunk.startOffset = lastChunk.startOffset + lastChunk.used
		s.chunks = append(s.chunks, newChunk)
		lastChunk = newChunk
	}
	copy(lastChunk.data[lastChunk.used:], data)
	lastChunk.used += len(data)
}

func (s *intSlice) Get(idx int) int {
	c, _ := s.findChunk(idx)
	return c.data[idx-c.startOffset]
}

func (s *intSlice) Set(idx int, value int) {
	c, _ := s.findChunk(idx)
	c.data[idx-c.startOffset] = value
}

func (s *intSlice) lastChunk() *intChunk {
	return s.chunks[len(s.chunks)-1]
}

func (s *intSlice) findChunk(end int) (*intChunk, int) {
	idx := end / byteChunkSize
	for s.chunks[idx].endOffset() < end {
		idx++
	}
	return s.chunks[idx], idx
}
