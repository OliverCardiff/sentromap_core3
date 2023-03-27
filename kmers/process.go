package kmers

import "sync"

type postionKmers struct {
	kmx      []uint64
	position int
}

// forwardExtract assumes the user has already provided a vec of u64 of at least len(seq)-(K-1)
func forwardExtract(sequence []byte, results []uint64) {

	var kmer uint64
	var km1 int = K - 1
	kmer = canoncial64(kmerAt(sequence[:8]))
	results[0] = kmer

	for i := K; i < len(sequence)-1; i++ {
		kmer = ((kmer << 2) | positionZero[sequence[i]]) & slideMask
		results[i-km1] = canoncial64(kmer)
	}
}

func convertInSegments(seqChan <-chan *segment, kmerChan chan<- postionKmers, wg *sync.WaitGroup) {

	for s := range seqChan {
		kmers := make([]uint64, len(s.seq)-(K-1))
		forwardExtract(s.seq, kmers)
		s.freeIt()
		kmerChan <- postionKmers{kmx: kmers, position: s.start}
	}

	wg.Done()
}

func GenomeToKset(genomeFile, ksetFile, tmpFolder string, threads int) error {

	fChan, divs, err := ReadFasta(genomeFile)
	if err != nil {
		return err
	}
	kChan := make(chan postionKmers, threads)

	ks, err := newKSConstructor(tmpFolder)
	if err != nil {
		return err
	}

	var wg1 sync.WaitGroup
	var wg2 sync.WaitGroup

	if err != nil {
		return err
	}

	for i := 0; i < threads; i++ {
		wg1.Add(1)
		go convertInSegments(fChan, kChan, &wg1)
	}

	wg2.Add(1)
	go ks.channelChunks(kChan, &wg2)

	wg1.Wait()
	close(kChan)
	wg2.Wait()
	err = ks.closeWrite()
	if err != nil {
		return err
	}

	ks.sortAndSave(threads)
	err = ks.reOrgsToKset(ksetFile, <-divs)
	if err != nil {
		return err
	}

	return nil
}
