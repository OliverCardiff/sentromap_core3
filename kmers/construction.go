package kmers

import (
	"encoding/gob"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"sync"
)

const PAGEMEM = 10000
const powerof4 = 4096
const kpShift = (K * 2) - 12

type KSConstructor struct {
	pages  [powerof4]*pager
	reorgs [powerof4]string
}

// newKSConstuctor assumes that you already have made the folder
func newKSConstructor(folder string) (*KSConstructor, error) {

	var ks KSConstructor

	var err error
	var path string
	for i := range ks.pages {
		path = filepath.Join(folder, fmt.Sprintf("tmp_%d", i))
		ks.pages[i], err = newPager(path)
		if err != nil {
			return nil, err
		}

		ks.reorgs[i] = filepath.Join(folder, fmt.Sprintf("reorg_%d", i))
	}

	return &ks, nil
}

func (ks *KSConstructor) closeWrite() error {
	var err error
	for i := range ks.pages {
		err = ks.pages[i].closeFromWrite()
		if err != nil {
			return err
		}
	}
	return err
}

func (ks *KSConstructor) channelChunks(pkChan <-chan postionKmers, wg *sync.WaitGroup) {

	for kp := range pkChan {
		for i, v := range kp.kmx {
			ks.pages[v>>kpShift].add(v, uint64(kp.position+i))
		}
	}
}

type midSort struct {
	k uint64
	p uint64
}

type midSlice []midSort

func (fs midSlice) Less(i, j int) bool { return fs[i].k < fs[j].k }
func (fs midSlice) Len() int           { return len(fs) }
func (fs midSlice) Swap(i, j int)      { fs[i], fs[j] = fs[j], fs[i] }

func (ks *KSConstructor) saveKChunk(path string, Ks []uint64, Ps [][]uint64) error {

	var (
		ge1 *gob.Encoder
		ge2 *gob.Encoder
		fh  *os.File
		err error
	)
	fh, err = os.Create(path)
	if err != nil {
		return err
	}
	defer fh.Close()

	ge1 = gob.NewEncoder(fh)
	err = ge1.Encode(&Ks)
	if err != nil {
		return err
	}
	ge2 = gob.NewEncoder(fh)
	err = ge2.Encode(&Ps)
	if err != nil {
		return err
	}

	return nil
}

func (ks *KSConstructor) convertMidToFinal(mids midSlice, Ks []uint64, Ps [][]uint64) ([]uint64, [][]uint64) {

	var (
		prev    uint64
		current []uint64
	)

	for i := range mids {
		if mids[i].k != prev {
			Ks = append(Ks, prev)
			Ps = append(Ps, current)

			current = make([]uint64, 1, 2)
			current[0] = mids[i].p

		} else {
			current = append(current, mids[i].p)
		}

		prev = mids[i].k
	}
	Ks = append(Ks, prev)
	Ps = append(Ps, current)

	return Ks, Ps
}

func (ks *KSConstructor) sortSaveWorker(iChan chan int, wg *sync.WaitGroup) {

	mids := make(midSlice, 0, 1e6)
	Ks := make([]uint64, 0, 1e6)
	Ps := make([][]uint64, 0, 1e6)

	var err error
	for i := range iChan {

		mids, err = ks.pages[i].readBackToMidSlice(mids)
		if err != nil {
			log.Println(err.Error())
			mids = mids[:0]
			continue
		}
		sort.Sort(mids)
		Ks, Ps = ks.convertMidToFinal(mids, Ks, Ps)
		err = ks.saveKChunk(ks.reorgs[i], Ks, Ps)
		Ks = Ks[:0]
		Ps = Ps[:0]

		if err != nil {
			log.Println(err.Error())
			continue
		}
		err = ks.pages[i].delete()
		if err != nil {
			log.Println(err.Error())
		}
	}
	wg.Done()
}

func (ks *KSConstructor) sortAndSave(threads int) {

	iChan := make(chan int, threads)
	var wg sync.WaitGroup

	go func() {
		for i := range ks.pages {
			iChan <- i
		}
		close(iChan)
	}()

	for i := 0; i < threads; i++ {
		wg.Add(1)
		go ks.sortSaveWorker(iChan, &wg)
	}

	wg.Wait()
}

func (ks *KSConstructor) recoverFromReorg(file string, Ks []uint64, Ps [][]uint64) ([]uint64, [][]uint64, error) {

	return Ks, Ps, nil
}

func (ks *KSConstructor) reOrgsToKset(ksFile string, divs []int64) error {

	var (
		kset *Kset
		err  error
		Karr []uint64
		Parr [][]uint64
	)
	kset = NewKset(ksFile)
	err = kset.openWrite()
	if err != nil {
		return err
	}

	Karr = make([]uint64, 0, 1e6)
	Parr = make([][]uint64, 0, 1e6)

	for i := range ks.reorgs {
		Karr, Parr, err = ks.recoverFromReorg(ks.reorgs[i], Karr, Parr)
		if err != nil {
			return err
		}

		err = kset.Add(i, Karr, Parr)
		if err != nil {
			return err
		}
	}

	err = kset.saveHeader(divs)
	if err != nil {
		return err
	}

	return nil
}
