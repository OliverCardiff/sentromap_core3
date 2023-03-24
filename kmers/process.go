package kmers

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

func ConvertInSegments(seqChan <-chan []byte, kmerChan chan<- []uint64, size int) {

	for s := range seqChan {
		kmers := make([]uint64, size)
		forwardExtract(s, kmers)
		kmerChan <- kmers
	}
}
