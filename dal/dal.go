package dal

import (
	"errors"
	"fmt"
	"os"

	"github.com/key-value-db/constants"
)

// data access layer
type DAL struct{
	File *os.File	// This file acts as a database
	PageSize int 	// size of each page in database
	FreeList *FreeList
	Meta *Meta
}

type Pgnum uint64	// acts as id for page

// page
type Page struct{
	Num Pgnum	// acts as id for page
	Data []byte
}

// open data access layer
// freelist and meta are empty initially
func CreateDAL(path string, pageSize int) (*DAL, error) {

	dal := &DAL{PageSize: pageSize}

	_, err := os.Stat(path)
	// if path exist read freelist, meta from path 
	// else create path and write freelist page and meta page
	if err == nil{
		
		dal.File, err = os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0666)
		if err!=nil{
			return nil, err
		}

		meta, err := dal.ReadMeta()
		if err!= nil{
			return nil, err
		}
		dal.Meta = meta

		freelist, err := dal.ReadFreeList()
		if err!= nil{
			return nil, err
		}
		dal.FreeList = freelist
	} else if errors.Is(err, os.ErrNotExist){
		dal.File, err = os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0666)
		if err!=nil{
			_ = dal.CloseDAL()
			return nil, err
		}

		dal.Meta = CreateEmptyMeta()
		dal.FreeList = CreateFreeList()
		dal.Meta.freelistPage = dal.FreeList.GetNextPage()
		
		_, err = dal.WriteFreeList()
		if err!= nil {
			return nil, err
		}
		
		_, err = dal.WriteMeta(dal.Meta)
		if err!= nil{
			return nil, err
		}
	} else {
		return nil, err
	}

	return dal, nil
}

// close data access layer
// closes file
func(dal *DAL) CloseDAL() error {

	if dal.File!=nil{
		
		err := dal.File.Close()
		if err!=nil {
			return fmt.Errorf("error while closing file: %s", err)
		}

		dal.File = nil
	}

	return nil
}

// empty page
func(dal *DAL) CreatePage() *Page{

	return &Page{Data: make([]byte, dal.PageSize)}
}

// read from file to a page
func(dal *DAL) ReadPage(pgnum Pgnum) (*Page, error) {

	// create empty page and read data to it from offset
	page := dal.CreatePage()
	offset := int(pgnum)*dal.PageSize

	_, err := dal.File.ReadAt(page.Data, int64(offset))
	if err!=nil{
		return nil, err
	}

	return page, nil
}

// write a page to file
func(dal *DAL) WritePage(page *Page) error{

	offset := int(page.Num)*dal.PageSize
	_, err := dal.File.WriteAt(page.Data, int64(offset))
	
	return err
}

// write meta page to file
func(dal *DAL) WriteMeta(meta *Meta) (*Page, error){

	page := dal.CreatePage()
	page.Num = constants.METAPAGENO
	
	meta.SerializeMeta(page.Data)
	err := dal.WritePage(page)
	if err!=nil {
		return nil, err
	} 

	return page, nil
}

// read meta from file to a page
func(dal *DAL) ReadMeta() (*Meta, error) {

	page, err := dal.ReadPage(Pgnum(constants.METAPAGENO))
	if err!=nil{
		return nil, err
	}

	meta := CreateEmptyMeta()
	meta.DeserializeMeta(page.Data)
	return meta, nil
}

// write freelist page to file
func(dal *DAL) WriteFreeList() (*Page, error){

	page := dal.CreatePage()
	page.Num = dal.Meta.freelistPage
	dal.FreeList.SerializeFreeList(page.Data)

	err:= dal.WritePage(page)
	if err!=nil{
		return nil, err
	}

	return page, nil
}

// read freelist from file to page
func(dal *DAL) ReadFreeList() (*FreeList, error){

	page, err := dal.ReadPage(dal.Meta.freelistPage)
	if err!= nil{
		return nil, err
	}

	freelist := CreateFreeList()
	freelist.DeserializeFreeList(page.Data)

	return freelist, nil
}