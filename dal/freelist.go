package dal

import (
	"encoding/binary"

	"github.com/key-value-db/constants"
)

type FreeList struct {
	Maxpage       Pgnum
	ReleasedPages []Pgnum
}

func CreateFreeList() *FreeList {
	return &FreeList{
		Maxpage:       constants.METAPAGENO,
		ReleasedPages: []Pgnum{},
	}
}

// if released pages is empty returns current maxpage incremented
// else return last page in released pages and remove that
func (freelist *FreeList) GetNextPage() Pgnum {

	if len(freelist.ReleasedPages) > 0 {

		pgnum := freelist.ReleasedPages[len(freelist.ReleasedPages)-1]
		freelist.ReleasedPages = freelist.ReleasedPages[:len(freelist.ReleasedPages)-1]
		return pgnum
	}

	freelist.Maxpage += 1
	return freelist.Maxpage
}

// adding a page to released pages
func (freelist *FreeList) ReleasedPage(pgnum Pgnum) {

	freelist.ReleasedPages = append(freelist.ReleasedPages, pgnum)
}

// serialise freelist
func (freelist *FreeList) SerializeFreeList(buf []byte) []byte {
	pos := 0
	binary.LittleEndian.PutUint16(buf[pos:], uint16(freelist.Maxpage))
	pos += constants.INT16SIZE

	binary.LittleEndian.PutUint16(buf[pos:], uint16(len(freelist.ReleasedPages)))
	pos += constants.INT16SIZE

	for _, page := range freelist.ReleasedPages{
		
		binary.LittleEndian.PutUint64(buf[pos:], uint64(page))
		pos += constants.PAGENUMSIZE
	}

	return buf
}

// deserialize freelist
func (freelist *FreeList) DeserializeFreeList(buf []byte) {
	pos := 0
	freelist.Maxpage = Pgnum(binary.LittleEndian.Uint16(buf[pos:]))
	pos += constants.INT16SIZE

	lengthOfReleasedPages := Pgnum(binary.LittleEndian.Uint16(buf[pos:]))
	pos += constants.INT16SIZE

	for i:=0; i<int(lengthOfReleasedPages); i++ {
		
		freelist.ReleasedPages = append(freelist.ReleasedPages, Pgnum(binary.LittleEndian.Uint64(buf[pos:])))
		pos += constants.PAGENUMSIZE
	}
}
