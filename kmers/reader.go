package kmers

import (
	"bufio"
	"os"
)

const SEGMENTSIZE = 100000

type segment struct {
	seq   []byte
	start int
	free  bool
}

type segmentMemory struct {
	slice []*segment
	iter  int
}

func NewSegmentMemory(count, segSize int) *segmentMemory {

	var s segmentMemory

	s.slice = make([]*segment, count)
	for i := range s.slice {
		s.slice[i].seq = make([]byte, segSize)
		s.slice[i].free = true
	}

	return &s
}

func (s *segmentMemory) nextFree() *segment {

	for !s.slice[s.iter].free {
		s.iter++
		if s.iter >= len(s.slice) {
			s.iter = 0
		}
	}

	return s.slice[s.iter]
}

func fastaReadRoutine(scanner *bufio.Scanner, divPoints chan []uint32,
	segChan chan *segment, fh *os.File) {

	var (
		line            []byte
		divisionCounter uint32
		divisions       []uint32
		mem             *segmentMemory
	)

	mem = NewSegmentMemory(256, SEGMENTSIZE)

	for scanner.Scan() {
		line = scanner.Bytes()

		if line[0] == '>' {
			if divisionCounter != 0 {
				divisions = append(divisions, divisionCounter)
			}
			continue
		}

		nxt := mem.nextFree()

		//TODO - fill in the segment

		segChan <- nxt

	}
	divPoints <- divisions
	fh.Close()
	close(segChan)
}

func ReadFasta(file string) (chan *segment, chan []uint32, error) {

	fh, err := os.Open(file)
	if err != nil {
		return nil, nil, err
	}
	ch := make(chan *segment)
	divPoints := make(chan []uint32)
	scanner := bufio.NewScanner(fh)

	go fastaReadRoutine(scanner, divPoints, ch, fh)

	return ch, divPoints, nil
}
