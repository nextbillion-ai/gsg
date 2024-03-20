package main

import (
	"bytes"
	"fmt"

	"github.com/nextbillion-ai/gsg/lib/object"
)

func main() {

	testContent := "test"
	testUrl := "gs://nb-data/chang/test.write"
	testUrl2 := "gs://nb-data/chang/test.write2"
	var err error
	/*
		run following when you removed envs or moved all aws credentials away, to test quit early
		change s3://whocares to gs://whocares if you unset GOOGLE_APPLICATION_CREDENTIALS
		{
			if _, err = object.New("s3://whocares"); err == nil {
				panic("should have error")
			}
			println(err.Error())

		}
	*/

	{
		buf := bytes.NewBuffer([]byte(testContent))
		var o *object.Object
		if o, err = object.New(testUrl); err != nil {
			panic(err)
		}
		if err = o.Write(buf); err != nil {
			panic(err)
		}
		if err = o.Reset(testUrl2); err != nil {
			panic(err)
		}
		buf = bytes.NewBuffer([]byte(testContent))
		if err = o.Write(buf); err != nil {
			panic(err)
		}
	}
	{
		buf := bytes.NewBuffer(nil)
		var o *object.Object
		if o, err = object.New(testUrl); err != nil {
			panic(err)
		}
		if err = o.Read(buf); err != nil {
			panic(err)
		}
		if buf.String() != testContent {
			panic("content not matching")
		}
	}
	{
		var files []*object.ObjectResult
		var o *object.Object
		if o, err = object.New(testUrl); err != nil {
			panic(err)
		}
		if files, err = o.List(false); err != nil {
			panic(err)
		}
		if len(files) != 1 || files[0].Url != testUrl {
			panic("list not matching")
		}
	}
	{
		var o *object.Object
		if o, err = object.New(testUrl); err != nil {
			panic(err)
		}
		if err = o.Delete(); err != nil {
			panic(err)
		}
		if err = o.Reset(testUrl2); err != nil {
			panic(err)
		}
		if err = o.Delete(); err != nil {
			panic(err)
		}
	}
	fmt.Println("all ok")
}
