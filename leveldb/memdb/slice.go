package memdb

const (
	byteOffShift  = 21
	byteChunkSize = 1 << byteOffShift
	intOffShift   = 18
	intChunkSize  = 1 << intOffShift
)

type byteChunk struct {
	startOffset int
	endOffset   int
	data        [byteChunkSize]byte
}

func (c *byteChunk) remain() int {
	return len(c.data) - (c.endOffset - c.startOffset)
}

type byteSlice struct {
	chunks []*byteChunk
}

func (s *byteSlice) PreAppend(size int) int {
	if len(s.chunks) == 0 {
		return 0
	}
	lastChunk := s.lastChunk()
	if lastChunk.remain() < size {
		newChunk := new(byteChunk)
		newChunk.startOffset = lastChunk.endOffset
		newChunk.endOffset = newChunk.startOffset
		s.chunks = append(s.chunks, newChunk)
		lastChunk = newChunk
	}
	idx := len(s.chunks) - 1
	off := lastChunk.endOffset - lastChunk.startOffset
	return idx<<byteOffShift | off
}

func (s *byteSlice) Truncate(end int) {
	c, idx := s.findChunk(end)
	c.endOffset = end
	s.chunks = s.chunks[:idx+1]
}

func (s *byteSlice) Append(data []byte) {
	if len(s.chunks) == 0 {
		s.chunks = append(s.chunks, new(byteChunk))
	}
	lastChunk := s.lastChunk()
	copy(lastChunk.data[lastChunk.endOffset-lastChunk.startOffset:], data)
	lastChunk.endOffset += len(data)
}

func (s *byteSlice) Slice(start, end int) []byte {
	c, off := s.findChunk(start)
	l := end - start
	return c.data[off : off+l]
}

func (s *byteSlice) lastChunk() *byteChunk {
	return s.chunks[len(s.chunks)-1]
}

func (s *byteSlice) findChunk(start int) (*byteChunk, int) {
	idx := start >> byteOffShift
	return s.chunks[idx], start & (1<<byteOffShift - 1)
}

type intChunk struct {
	startOffset int
	endOffset   int
	data        [intChunkSize]int
}

func (c *intChunk) remain() int {
	return len(c.data) - (c.endOffset - c.startOffset)
}

type intSlice struct {
	chunks []*intChunk
}

func (s *intSlice) PreAppend(size int) int {
	if len(s.chunks) == 0 {
		return 0
	}
	lastChunk := s.lastChunk()
	if lastChunk.remain() < size {
		newChunk := new(intChunk)
		newChunk.startOffset = lastChunk.endOffset
		newChunk.endOffset = newChunk.startOffset
		s.chunks = append(s.chunks, newChunk)
		lastChunk = newChunk
	}
	idx := len(s.chunks) - 1
	off := lastChunk.endOffset - lastChunk.startOffset
	return idx<<intOffShift | off
}

func (s *intSlice) Truncate(end int) {
	c, idx := s.findChunk(end)
	c.endOffset = end
	s.chunks = s.chunks[:idx+1]
}

func (s *intSlice) Append(data []int) {
	if len(s.chunks) == 0 {
		s.chunks = append(s.chunks, new(intChunk))
	}
	lastChunk := s.lastChunk()
	copy(lastChunk.data[lastChunk.endOffset-lastChunk.startOffset:], data)
	lastChunk.endOffset += len(data)
}

func (s *intSlice) Get(idx int) int {
	c, off := s.findChunk(idx)
	return c.data[off]
}

func (s *intSlice) Set(idx int, value int) {
	c, off := s.findChunk(idx)
	c.data[off] = value
}

func (s *intSlice) lastChunk() *intChunk {
	return s.chunks[len(s.chunks)-1]
}

func (s *intSlice) findChunk(start int) (*intChunk, int) {
	idx := start >> intOffShift
	return s.chunks[idx], start & (1<<intOffShift - 1)
}
