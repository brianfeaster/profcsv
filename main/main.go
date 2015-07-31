package main

import (
	"fmt"
	"errors"
	"encoding/csv"
)


type SimpleFile struct {
  data string;
     i int;
}

func SimpleFileNew (d string) *SimpleFile {
	return &SimpleFile{data:d, i:0}
}

func (this *SimpleFile) Read(p []byte) (n int, err error) {
	if len(this.data) <= this.i {
		n = 0;
		err = errors.New("No more data!")
	} else {
		p[0] = this.data[this.i]
		this.i++
		n = 1
		err = nil
	}
	return
}

func main () {
	var strs []string
	var err error = nil;
	r := csv.NewReader(SimpleFileNew(
		"ab,cd,12\n" +
		"AB,CD,12,34\n" +
		"x,y,z\n"))
	for {
		strs, err = r.Read()
		fmt.Printf("Output:%v\n", strs)
		if err!=nil { break }
	}
	fmt.Print(err)
}
