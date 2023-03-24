package kmers

import "encoding/binary"

var K = 31                        // the globally utilised size of K that should be used everywhere
var bFlip [256]byte               // reverse complement lookup table in chunks of 4 bases
var invK uint32                   // the distance in bits to shift a post-rev-comp kmer that isn't the same size as the integer containing it
var positionFlips [32][256]uint64 // lookup table to create the bit-flip positions
var positionZero [256]uint64      // single position lookup
var slideMask uint64              // mask to remove most significant bits when slide-by-2 kmer-generating

func init() {
	genRevC()
	invK = uint32((32 - K) << 1)
	generateAllPositionalFlips()
	slideMask = (^uint64(0)) >> ((32 - K) << 1)
}

func genRevC() {
	revC := []int{3, 2, 1, 0}
	for i := 0; i < 4; i++ {
		for j := 0; j < 4; j++ {
			for k := 0; k < 4; k++ {
				for l := 0; l < 4; l++ {
					valval := byte(i + (j * 4) + (k * 16) + (l * 64))
					inval := byte(revC[l] + (revC[k] * 4) + (revC[j] * 16) + (revC[i] * 64))
					bFlip[valval] = inval
				}
			}
		}
	}
}

// canoncial64 return the canoncial version of a kmer (least numerical vs reverse complement)
func canoncial64(kmer uint64) uint64 {
	var (
		fb8 [8]byte
		rc8 [8]byte
		rev uint64
	)

	binary.LittleEndian.PutUint64(fb8[:], kmer)

	rc8[7] = bFlip[fb8[0]]
	rc8[6] = bFlip[fb8[1]]
	rc8[5] = bFlip[fb8[2]]
	rc8[4] = bFlip[fb8[3]]
	rc8[3] = bFlip[fb8[4]]
	rc8[2] = bFlip[fb8[5]]
	rc8[1] = bFlip[fb8[6]]
	rc8[0] = bFlip[fb8[7]]

	rev = binary.LittleEndian.Uint64(rc8[:]) >> invK

	if rev < kmer {
		return rev
	}
	return kmer
}

// kmerAt always assumes the user is giving a slice at least K in length
func kmerAt(bs []byte) uint64 {
	var kx uint64 = 0

	for i, j := K-1, 0; i >= 0; i, j = i-1, j+1 {
		kx |= positionFlips[i][bs[j]]
	}

	return kx
}

func generateAllPositionalFlips() {
	for i := 0; i < 32; i++ {
		generatePositionalLookup(i)
	}
}

func generatePositionalLookup(pos int) {

	rv := positionFlips[pos]
	bVal := uint64(1) << (pos << 1)

	for i := 0; i <= 255; i++ {
		if i == 'A' || i == 'a' {
			rv[i] = 0
		} else if i == 'C' || i == 'c' {
			rv[i] = bVal
		} else if i == 'G' || i == 'g' {
			rv[i] = bVal * 2
		} else if i == 'T' || i == 't' {
			rv[i] = bVal * 3
		} else if i == 'U' || i == 'u' {
			rv[i] = bVal * 3
		} else {
			rv[i] = 0
		}
	}
	positionFlips[pos] = rv

	if pos == 0 {
		positionZero = rv
	}
}
