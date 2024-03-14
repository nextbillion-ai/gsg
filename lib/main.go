package main

import (
	"bytes"
	"fmt"

	"github.com/nextbillion-ai/gsg/lib/object"
)

func main() {

	testContent := "test"
	testUrl := "gs://nb-data/chang/test.write"
	var err error

	{
		buf := bytes.NewBuffer([]byte(testContent))
		if err = object.Write(testUrl, buf); err != nil {
			panic(err)
		}
	}
	{
		buf := bytes.NewBuffer(nil)
		if err = object.Read(testUrl, buf); err != nil {
			panic(err)
		}
		if buf.String() != testContent {
			panic("content not matching")
		}
	}
	{
		var files []string
		if files, err = object.List(testUrl, false); err != nil {
			panic(err)
		}
		if len(files) != 1 || files[0] != testUrl {
			panic("list not matching")
		}
	}
	{
		if err = object.Delete(testUrl); err != nil {
			panic(err)
		}
	}
	fmt.Println("all ok")
}
