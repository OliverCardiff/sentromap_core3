package kmers

import (
	"encoding/gob"
	"io"
	"os"
)

type Kset struct {
	kIndex [powerof4]int64 // the file positions of the chunks of sequence
	pIndex [powerof4]int64
	divs   []int64
	file   string
	header string
	fh     *os.File
}

func NewKset(filename string) *Kset {
	var k Kset
	k.file = filename
	k.header = filename + ".header"
	k.fh = nil

	return &k
}

func LoadKsetFrom(file string) (*Kset, error) {

	var k Kset

	k.file = file
	header := file + ".header"
	fh, err := os.Open(header)
	if err != nil {
		return nil, err
	}
	defer fh.Close()

	gd := gob.NewDecoder(fh)
	err = gd.Decode(&k.kIndex)
	if err != nil {
		return nil, err
	}
	err = gd.Decode(&k.pIndex)
	if err != nil {
		return nil, err
	}

	err = gd.Decode(&k.divs)
	if err != nil {
		return nil, err
	}

	k.fh, err = os.Open(k.file)
	if err != nil {
		return nil, err
	}

	return &k, nil
}

func (k *Kset) saveHeader(divs []int64) error {

	fh, err := os.Create(k.header)
	if err != nil {
		return err
	}
	defer fh.Close()

	ge := gob.NewEncoder(fh)
	err = ge.Encode(&k.kIndex)
	if err != nil {
		return err
	}
	err = ge.Encode(&k.pIndex)
	if err != nil {
		return err
	}
	err = ge.Encode(&divs)
	if err != nil {
		return err
	}

	return nil
}

func (k *Kset) openWrite() error {
	var err error
	k.fh, err = os.Create(k.file)
	if err != nil {
		return err
	}

	return nil
}

func (k *Kset) Close() error {
	return k.fh.Close()
}

// Add assumes we are getting the 4096 chunks in order
func (k *Kset) Add(i int, kmers []uint64, positions [][]uint64) error {
	pos, err := k.fh.Seek(0, io.SeekCurrent)
	if err != nil {
		return err
	}
	k.kIndex[i] = pos

	g1 := gob.NewEncoder(k.fh)
	err = g1.Encode(kmers)
	if err != nil {
		return err
	}

	pos, err = k.fh.Seek(0, io.SeekCurrent)
	if err != nil {
		return err
	}
	k.pIndex[i] = pos

	g2 := gob.NewEncoder(k.fh)
	err = g2.Encode(positions)
	if err != nil {
		return err
	}

	return nil
}

func (k *Kset) GetKAt(i int) ([]uint64, error) {

	var (
		ret []uint64
		err error
	)

	_, err = k.fh.Seek(k.kIndex[i], io.SeekStart)
	if err != nil {
		return nil, err
	}

	gd := gob.NewDecoder(k.fh)
	err = gd.Decode(&ret)
	if err != nil {
		return nil, err
	}

	return ret, nil
}

func (k *Kset) GetPAt(i int) ([][]uint64, error) {

	var (
		ret [][]uint64
		err error
	)

	_, err = k.fh.Seek(k.pIndex[i], io.SeekStart)
	if err != nil {
		return nil, err
	}

	gd := gob.NewDecoder(k.fh)
	err = gd.Decode(&ret)
	if err != nil {
		return nil, err
	}

	return ret, nil
}

func (k *Kset) GetAllAtI(i int) ([]uint64, [][]uint64, error) {

	var (
		ks  []uint64
		ps  [][]uint64
		err error
	)

	ks, err = k.GetKAt(i)
	if err != nil {
		return nil, nil, err
	}
	ps, err = k.GetPAt(i)
	if err != nil {
		return nil, nil, err
	}

	return ks, ps, nil
}
