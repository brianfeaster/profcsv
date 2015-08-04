package main

import (
	"bytes"
	"encoding/csv"
	"runtime"
	"fmt"
 "io"
	"net/http"
	"net/url"
	"bufio"
	"os"
	"strconv"
	"strings"
	"sync"
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
		f = len(strs)
		if f < FieldMax {
			counts[f]++
		} else {
			fmt.Println("ERROR: more than", FieldMax, "fields")
		}
		if err != nil { break }
	}
	fmt.Print(header)
	for f=0; f<FieldMax; f++ {
		if 0 < counts[f] { fmt.Println(f, counts[f]) }
	}

	var t1 time.Time = time.Now()

	fmt.Printf("The call took %f seconds.\n\n", t1.Sub(t0).Seconds())
}

func countFieldsNEW(reader *csv.Reader, header string) {
	var f int
	var counts [FieldMax]int
	//var strs []string
	var err error
	reader.FieldsPerRecord = 227

	var t0 time.Time = time.Now()

	err = nil
	for {
		_, err = reader.Read()
		if err != nil {
			break
		}
	}
	fmt.Print(header)
	for f = 0; f < FieldMax; f++ {
		if 0 < counts[f] {
			fmt.Println(f, counts[f]) } }

	var t1 time.Time = time.Now()

	fmt.Printf("The call took %f seconds.\n\n", t1.Sub(t0).Seconds())
}

func worker(wg *sync.WaitGroup, purl *url.URL, i int64, rng string) {
	defer wg.Done()
	filename := fmt.Sprintf("data-%015d", i)
	fmt.Printf("%s: getting range %s\n", filename, rng)
	c := &http.Client{}
	resp, err := c.Do(&http.Request{URL: purl, Header: http.Header{"Range": []string{rng}}})
	if err != nil {
		panic(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 206 {
		panic(fmt.Errorf("non 206: %s\n", resp))
	}
	serversays := strings.Split(strings.Split(resp.Header["Content-Range"][0], "/")[0], "-")
	serversays[0] = strings.TrimPrefix(serversays[0], "bytes ")
	if len(serversays) == 2 && serversays[0] == serversays[1] {
		fmt.Printf("%s: empty\n", filename)
		return
	}
	now := time.Now()
	o, err := os.Create(filename)
	defer o.Close()
	if err != nil {
		panic(err)
	}
	numbytes, err := io.Copy(o, resp.Body)
	csvreader := csv.NewReader(bufio.NewReaderSize(resp.Body, 32767))
	csvreader.FieldsPerRecord = -1
	for {
		_, err := csvreader.Read()
		if err != nil {
			break
		}
	}
	if err != nil { panic(err) }
	fmt.Printf("\"%s\"  bytes:%d  duration:%s  len:%s  content-range:%s\n", filename, numbytes, time.Since(now), resp.Header["Content-Length"][0], resp.Header["Content-Range"][0])
}



/* Exercises the countFields function on a simple string derived CSV.
*/
func testReadString () {
	var reader *csv.Reader

	reader = csv.NewReader(bytes.NewReader([]byte("ab,cd,12\nAB,CD,12,34\nx,y,z\n")))
	countFields(reader, "static string")
}


/* Exercises the countFields function on a local CSV file.
*/
func testReadFile () {
	var reader *csv.Reader
	var err error
	var f *os.File
	var fileReader *bufio.Reader

	f, err = os.Open("ss10pusa.csv")
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


func testSplit() {
	threadCount := runtime.NumCPU()
	runtime.GOMAXPROCS(threadCount)
	var err error
	u := "http://west.xfltr.com.s3-us-west-2.amazonaws.com/census/ss10pusa.csv"
	urlParsed, err := url.Parse(u)
//fmt.Println("::urlParsed", urlParsed)
	if err != nil {
		panic(err)
	}
	urlHeader, err := http.Head(u)
	if err != nil {
		panic(err)
	}
//fmt.Println("::urlHeader", urlHeader)
//for a, b := range(urlHeader.Header) { fmt.Println("::urlHeader", a, b) }
	leng, err := strconv.ParseInt(urlHeader.Header["Content-Length"][0], 10, 64)
	fmt.Printf("length = %d\n", leng)
	sz := leng / int64(threadCount)
	wg := sync.WaitGroup{}
	started := time.Now()
	for i := int64(0); i < leng; i += sz {
		upper := i + sz -1
		done := false
		if upper > leng {
			upper = leng
			done = true
		}
		rng := fmt.Sprintf("bytes=%d-%d", i, upper)
		wg.Add(1)
		go worker(&wg, urlParsed, i, rng)
		if done { break }
	}
	fmt.Printf("waiting...\n")
	wg.Wait()
	fmt.Printf("took %s\n", time.Since(started))
}

func main() {
	//testReadString()
	//testReadFile()
	testSplit();
}
