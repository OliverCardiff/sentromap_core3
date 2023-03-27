package kmers

import (
	"bufio"
	"os"
)

const SEGMENTSIZE = 100000
const SEGPLUS = SEGMENTSIZE + (K - 1)

type segment struct {
	seq   []byte
	start int
	free  bool
}

func (s *segment) add(seq []byte) ([]byte, bool) {

	if len(s.seq) < SEGPLUS {
		diff := SEGPLUS - len(s.seq)
		if diff <= len(seq) {
			s.seq = append(s.seq, seq[:diff]...)
			return seq[diff:], true // return the unused sequence, say we are full
		} else {
			s.seq = append(s.seq, seq...)
			return seq[:0], false // return no unused sequence, say we aren't full
		}
	}
	return seq, true // we didn't need the sequence, already full
}

func (s *segment) holdOver() []byte {
	return s.seq[SEGMENTSIZE:]
}

func (s *segment) freeIt() {
	s.free = true
	s.seq = s.seq[:0]
}

type segmentMemory struct {
	slice []*segment
	iter  int
}

func NewSegmentMemory(count, segSize int) *segmentMemory {

	var s segmentMemory

	s.slice = make([]*segment, count)
	for i := range s.slice {
		s.slice[i].seq = make([]byte, 0, segSize)
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

func fastaReadRoutine(scanner *bufio.Scanner, divPoints chan []int64,
	segChan chan *segment, fh *os.File) {

	var (
		hold            []byte
		extra           []byte
		filled          bool
		line            []byte
		divisionCounter int
		divisions       []int64
		current         *segment
		mem             *segmentMemory
	)

	mem = NewSegmentMemory(256, SEGPLUS)

	for scanner.Scan() {
		line = scanner.Bytes()

		if line[0] == '>' {

			if len(current.seq) != 0 {
				divisionCounter += len(current.seq)
				segChan <- current
				current = mem.nextFree()
				current.start = divisionCounter
			}

			if divisionCounter != 0 {
				divisions = append(divisions, int64(divisionCounter))
			}

			continue
		}

		extra, filled = current.add(line)

		for filled {

			divisionCounter += SEGMENTSIZE
			hold = current.holdOver()
			segChan <- current

			current = mem.nextFree()
			divisionCounter += len(line)
			current.start = int(divisionCounter)

			current.add(hold)
			extra, filled = current.add(extra)
		}
	}

	if len(current.seq) != 0 {
		divisionCounter += len(current.seq)
		segChan <- current
	}

	divPoints <- divisions
	fh.Close()
	close(segChan)
}

func ReadFasta(file string) (chan *segment, chan []int64, error) {

	fh, err := os.Open(file)
	if err != nil {
		return nil, nil, err
	}
	ch := make(chan *segment)
	divPoints := make(chan []int64)
	scanner := bufio.NewScanner(fh)

	go fastaReadRoutine(scanner, divPoints, ch, fh)

	return ch, divPoints, nil
}
