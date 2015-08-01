package main

import (
	"bytes"
	"encoding/csv"
	"fmt"
	"bufio"
	"os"
	"time"
)

const FieldMax int = 500

func countFields (reader *csv.Reader, header string) {
	var f int
	var counts [FieldMax]int
	var strs []string
	var err error
	reader.FieldsPerRecord = -1

	var t0 time.Time = time.Now()

	err = nil
	for {
		strs, err = reader.Read()
		//fmt.Printf("Output:%v:%d\n", strs, len(strs))
		f = len(strs)
		if f < FieldMax {
			counts[f]++
		} else {
			fmt.Println("ERROR: more than", FieldMax, "fields")
		}
		if err != nil { break }
	}
	//fmt.Print(err)
	fmt.Print(header)
	for f=0; f<FieldMax; f++ {
		if 0 < counts[f] { fmt.Println(f, counts[f]) }
	}

	var t1 time.Time = time.Now()

	fmt.Printf("The call took %f seconds.\n\n", t1.Sub(t0).Seconds())
}

func main() {
	var reader *csv.Reader
	var err error

	// read string   /////////////////
	reader = csv.NewReader(bytes.NewReader([]byte("ab,cd,12\nAB,CD,12,34\nx,y,z\n")))
	countFields(reader, "static string")

	// read csv file /////////////////

	var f *os.File
	var fileReader *bufio.Reader

	f, err = os.Open("ss10pusa.csv")
	//f, err = os.Open("fun.csv")
	if err != nil {
		fmt.Println("Can't open csv file\n")
		return
	}

	fileReader = bufio.NewReader(f)
	if fileReader == nil {
		fmt.Println("Can't create reader out of file")
		return
	}

	reader = csv.NewReader(fileReader)
	countFields(reader, "file")

}
