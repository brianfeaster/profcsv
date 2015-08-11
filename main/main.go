package main

import (
	"bytes"
	"encoding/csv"
	"fmt"
	"github.com/brianfeaster/profcsv/bufrecs"
	"github.com/brianfeaster/profcsv/filechunk"
	"io"
	"net/http"
	"net/url"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Helpers ////////

func urlContentLength(url string) (len int64) {
	urlHeader, e := http.Head(url)
	if e != nil {
		len = -1.0
	} else {
		l, _ := strconv.ParseInt(urlHeader.Header["Content-Length"][0], 10, 64)
		len = int64(l)
	}
	return
}

func min(a, b int) int {
	if a < b {
		return a
	} else {
		return b
	}
}

func stringMin(a []byte) string {
	var l int = min(40, len(a))
	return string(a[0:l]) + " ... " + string(a[len(a)-l:len(a)])
}

func recordFlatten(rec [][]byte) (f []byte) {
	f = make([]byte, 0)
	for _, s := range rec {
		f = append(f, s...)
	}
	return
}

func createSplitLengths(length int64, count int64) (lengths []int64) {
	lengths = make([]int64, count)
	splitSize := length / count
	i := int64(0)
	for i < count-1 {
		lengths[i] = splitSize
		length = length - splitSize
		i++
	}
	lengths[i] = length
	return
}

func fileSize(filename string) (size int64, err error) {
	f, err := os.Open(filename)
	if err != nil {
		return
	}
	fs, err := f.Stat()
	if err != nil {
		return
	}
	size = fs.Size()
	f.Close()
	return
}

/* Given a CSV reader, tabulate the field counts over all the records.
 */
const FieldMax int = 500

func countFields(reader *csv.Reader, header string) {
	var f int
	var counts [FieldMax]int
	var strs []string
	var err error
	reader.FieldsPerRecord = -1

	var now time.Time = time.Now()

	err = nil
	for {
		strs, err = reader.Read()
		f = len(strs)
		if f < FieldMax {
			counts[f]++
		} else {
			fmt.Println("ERROR: more than", FieldMax, "fields")
		}
		if err != nil {
			break
		}
	}

	fmt.Print(header)
	for f = 0; f < FieldMax; f++ {
		if 0 < counts[f] {
			fmt.Println(f, counts[f])
		}
	}

	fmt.Println("Done.", time.Since(now))
}

/* Exercises the countFields function on a simple string derived CSV.
 */
func countCSVFieldsFromString(str []byte, desc string) {
	var reader *csv.Reader = csv.NewReader(bytes.NewReader(str))
	countFields(reader, desc)
}

/* Exercises the countFields function on a chunked ReadCloser
 */
func countCSVFieldsFromChunkedReader(wg *sync.WaitGroup, threadId int, r io.ReadCloser, partials [][]byte) {
	now := time.Now()
	var bufr *bufrecs.BufRecs = bufrecs.NewBufRecs(r) // Create the buffered record reader

	partials[threadId*2] = bufr.Get()
	csvreader := csv.NewReader(bufr) // Pass the remote file chunk to csv

	csvreader.FieldsPerRecord = -1
	countFields(csvreader, fmt.Sprintf("Thread_%d", threadId))

	r.Close() // Close the ReadCloser.  No longer needed now

	partials[threadId*2+1] = recordFlatten(bufr.FinalPartial) // Pass back the pre and post partials.

	fmt.Printf("Thread %d done. [%s]\n", threadId, time.Since(now))
	wg.Done()
}

/* Count the fields per record from a CSV via a local file
 */
func testFileSplit(threadCount int64, filename string) {
	var err error
	var from int64 = 0
	var to int64

	fileSize, err := fileSize(filename)
	if err != nil {
		panic(err)
	}

	lengths := createSplitLengths(fileSize, threadCount)

	fmt.Println("FileSize:", fileSize, "\nThread count:", len(lengths))

	wg := sync.WaitGroup{}
	partials := make([][]byte, threadCount*2) // Will contain each thread's pre and post partial records.

	for idx, chunkLength := range lengths {
		wg.Add(1)
		to = from + chunkLength
		f, _ := os.Open(filename) // f is closed in filechunk which is is closed in countCSVFieldsFromChunkedReader
		go countCSVFieldsFromChunkedReader(&wg, idx, filechunk.NewFileChunk(f, from, to), partials)
		from = to
	}

	wg.Wait()

	countCSVFieldsFromString(recordFlatten(partials), "Reassembled interchunk partials.")
}

/* Count the fields per record from a CSV via HTTP
 */

func testURISplitWorker(wg *sync.WaitGroup, urlParsed *url.URL, threadId int, from int64, to int64, partials [][]byte) {
	// Open http chunked stream
	rng := fmt.Sprintf("bytes=%d-%d", from, (to - 1)) // The http range header which specifies an inclusive range.
	fmt.Printf("%d:: %s %d\n", threadId, rng, to-from)

	// Open connection.  resp is a ReadCloser and is closed in countCSVFieldsFromChunkedReader
	client := &http.Client{}
	resp, err := client.Do(&http.Request{URL: urlParsed, Header: http.Header{"Range": []string{rng}}})
	if err != nil {
		panic(err)
	}

	if resp.StatusCode != 206 { // Consider response
		panic(fmt.Errorf("%d:: non 206: %s\n", threadId, resp))
	}

	serversays := strings.Split(strings.Split(resp.Header["Content-Range"][0], "/")[0], "-")
	fmt.Printf("%d:: serversays:%s\n", threadId, serversays)
	serversays[0] = strings.TrimPrefix(serversays[0], "bytes ")
	if len(serversays) == 2 && serversays[0] == serversays[1] {
		fmt.Printf("%d:: server reports empty\n", threadId)
		return
	}

	countCSVFieldsFromChunkedReader(wg, threadId, resp.Body, partials)
}

func testURISplit(uri string, threadCount int64) {
	var e error
	var urlLength, to, from int64

	urlLength = urlContentLength(uri)

	fmt.Printf("URL: %s\nContent-length: [%d %0.3f Gb]\n", uri, urlLength, float64(urlLength)/(1024.0*1024.0*1024.0))

	if urlLength < 0 {
		panic("Content length < 0.")
	}

	lengths := createSplitLengths(urlLength, threadCount)
	wg := sync.WaitGroup{}
	from = 0
	urlParsed, e := url.Parse(uri)
	if e != nil {
		panic(e)
	}
	partials := make([][]byte, threadCount*2) // Will contain each thread's pre and post partial records.

	for idx, chunkLength := range lengths {
		wg.Add(1)
		to = from + chunkLength
		go testURISplitWorker(&wg, urlParsed, idx, from, to, partials)
		from = to
	}

	fmt.Println("Waiting on", threadCount, "threads...")
	wg.Wait()

	countCSVFieldsFromString(recordFlatten(partials), "Reassembled interchunk partials.")
}

// Main ////////

var URIs = []string{
	"http://west.xfltr.com.s3-us-west-2.amazonaws.com/census/ss10pusa.csv", // 3.502 Gb
	"http://west.xfltr.com.s3-us-west-2.amazonaws.com/census/ss10pusd.csv", // 2.941 Gb
	"http://west.xfltr.com.s3-us-west-2.amazonaws.com/census/ss10pusc.csv", // 2.806 Gb
	"http://west.xfltr.com.s3-us-west-2.amazonaws.com/census/ss10pusb.csv"} // 2.643 Gb

func main() {
	var startTime time.Time = time.Now()
	var threadCount int64 = int64(runtime.NumCPU())
	runtime.GOMAXPROCS(int(threadCount))

	//countCSVFieldsFromString([]byte("ab,cd,12\nAB,CD,12,34\nx,y,z\n", "static string"))
	//testFileSplit(2, "ss10pusb.csv")
	testURISplit(URIs[3], threadCount*2)

	fmt.Printf("Done. [%s]\n", time.Since(startTime))
}
