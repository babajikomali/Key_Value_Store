package main

import (
	"fmt"
	"os"

	"github.com/key-value-db/dal"
)

func main(){

	dataAccessLayer, err := dal.CreateDAL("key-store.db", os.Getpagesize())
	if err!=nil {
		fmt.Printf("error while creating data access layer: %s", err)
	}
	
	page := dataAccessLayer.CreatePage()
	page.Num = dataAccessLayer.FreeList.GetNextPage()
	
	copy(page.Data[:], "babaji")

	err = dataAccessLayer.WritePage(page)
	if err!=nil{
		fmt.Printf("error while writing to page: %s", err)
	}
	_, err = dataAccessLayer.WriteFreeList()
	if err!=nil {
		fmt.Printf("error while writing to freelist: %s", err)
	}

	_ = dataAccessLayer.CloseDAL()
	if err!= nil {
		fmt.Printf("error while closing the data access layer: %s", err)
	}

	dataAccessLayer, err = dal.CreateDAL("key-store.db", os.Getpagesize())
	if err!=nil {
		fmt.Printf("error while creating data access layer: %s", err)
	}

	page = dataAccessLayer.CreatePage()
	page.Num = dataAccessLayer.FreeList.GetNextPage()
	copy(page.Data[:], "pattabhiram")

	err = dataAccessLayer.WritePage(page)
	if err!=nil{
		fmt.Printf("error while writing to page: %s", err)
	}
	_, err = dataAccessLayer.WriteFreeList()
	if err!=nil {
		fmt.Printf("error while writing to freelist: %s", err)
	}

	dataAccessLayer.FreeList.ReleasedPage(dal.Pgnum(3))

	page = dataAccessLayer.CreatePage()
	page.Num = dataAccessLayer.FreeList.GetNextPage()
	copy(page.Data[:], "gowd")

	err = dataAccessLayer.WritePage(page)
	if err!=nil{
		fmt.Printf("error while writing to page: %s", err)
	}
	_, err = dataAccessLayer.WriteFreeList()
	if err!=nil {
		fmt.Printf("error while writing to freelist: %s", err)
	}

	_ = dataAccessLayer.CloseDAL()
	if err!= nil {
		fmt.Printf("error while closing the data access layer: %s", err)
	}

}