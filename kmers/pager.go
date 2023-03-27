package kmers

import (
	"encoding/gob"
	"os"
)

type pager struct {
	fh       *os.File
	filename string
	memoryK  []uint64
	memoryP  []uint64
}

func newPager(loc string) (*pager, error) {

	var p pager
	var err error

	p.fh, err = os.Create(loc)
	if err != nil {
		return nil, err
	}
	p.filename = loc
	p.memoryK = make([]uint64, 0, PAGEMEM)
	p.memoryP = make([]uint64, 0, PAGEMEM)

	return &p, nil
}

func (p *pager) dump() error {
	var err error

	gd := gob.NewEncoder(p.fh)
	err = gd.Encode(p.memoryK)
	if err != nil {
		return err
	}
	err = gd.Encode(p.memoryP)
	if err != nil {
		return err
	}

	p.memoryK = p.memoryK[:0]
	p.memoryP = p.memoryP[:0]

	return nil
}

func (p *pager) add(k, pos uint64) error {

	if len(p.memoryK) == PAGEMEM {
		err := p.dump()
		if err != nil {
			return err
		}
	}

	p.memoryK = append(p.memoryK, k)
	p.memoryP = append(p.memoryP, pos)

	return nil
}

func (p *pager) closeFromWrite() error {

	var err error
	if len(p.memoryK) > 0 {
		err = p.dump()
		if err != nil {
			return err
		}
	}

	return p.fh.Close()
}

func (p *pager) delete() error {
	return os.Remove(p.filename)
}

func (p *pager) readBackToMidSlice(mid midSlice) (midSlice, error) {
	var (
		err error
		gd1 *gob.Decoder
		gd2 *gob.Decoder
		mp  []uint64
		mk  []uint64
	)

	p.fh, err = os.Open(p.filename)
	if err != nil {
		return nil, err
	}
	defer p.fh.Close()

	for {
		gd1 = gob.NewDecoder(p.fh)
		err = gd1.Decode(&mk)
		if err != nil {
			break
		}
		gd2 = gob.NewDecoder(p.fh)
		err = gd2.Decode(&mp)
		if err != nil {
			return nil, err
		}
		for i := range mk {
			mid = append(mid, midSort{k: mk[i], p: mp[i]})
		}
	}

	return mid, nil
}
