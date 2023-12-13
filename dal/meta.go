package dal

import (
	"encoding/binary"

	"github.com/key-value-db/constants"
)

type Meta struct{
	freelistPage Pgnum
}

func CreateEmptyMeta() *Meta{

	return &Meta{}
}

// serialise meta page number 
func(meta *Meta) SerializeMeta(buf []byte){
	pos := 0
	binary.LittleEndian.PutUint64(buf[pos:], uint64(meta.freelistPage))
	pos += constants.PAGENUMSIZE
} 

// deserialize meta page number
func(meta *Meta) DeserializeMeta(buf []byte){
	pos := 0
	meta.freelistPage = Pgnum(binary.LittleEndian.Uint64(buf[pos:]))
	pos += constants.PAGENUMSIZE
}

