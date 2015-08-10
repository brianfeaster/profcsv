package main

import (
	"bufio"
	"bytes"
	"encoding/csv"
	"errors"
	"fmt"
	"github.com/brianfeaster/profcsv/bufrecs"
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

// Helper s

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

/* NewLineReader -- Implements a ReaderCloser that will only return characters that end in newline.  It will keep track of initial characters (which can't be determined if they have been preceeded by a newline) and trailing characters that are not delmited by a newline.
 */
const NewlineReaderBufferSize = 4096

type NewlineReader struct {
	reader   io.ReadCloser
	buffSize int
	pre      []byte // inital bytes plus newline   [------/] or [-------]
	post     []byte // final bytes (no newline)    [-------]
	tosend   []byte
}

func NewNewlineReader(reader io.ReadCloser) *NewlineReader {
	return &NewlineReader{
		reader: reader,
		pre:    make([]byte, 0, NewlineReaderBufferSize),
		post:   make([]byte, 0, NewlineReaderBufferSize),
		tosend: make([]byte, 0, NewlineReaderBufferSize)}
}

/* Need to accumulate records in a queue

If pre empty:
	Read until a newline is seen.  Copy everythying to pre
*/

func (this *NewlineReader) Read(b []byte) (n int, err error) {

	// If anything to send, sent it out.
	if 0 < len(this.tosend) {
		fmt.Println("sending only...")
		fmt.Print(" SEND:", stringMin(this.tosend), "\n\n")
		n = copy(b, this.tosend)
		err = nil
		fmt.Println("sent ", n)
		this.tosend = this.tosend[n:len(this.tosend)]
		fmt.Print(" SEND:", stringMin(this.tosend), "\n\n")
		return
	}

	if 0 == len(this.pre) || '\n' != this.pre[len(this.pre)-1] { // Haven't found a newline yet.  Read more into it

		fmt.Print("pre not full...")
		l, e := this.reader.Read(b) // Use buffer passed in (will have a length, not sure of the capacity)

		if l <= 0 || e != nil { // If unable to read anything, yield
			n = l
			err = e
			fmt.Println("returning")
			return
		}

		offset := bytes.IndexByte(b[:l], '\n') // Look for a newline
		if 0 <= offset {                       // Found a newline so keep track in pre and the rest goes in post
			fmt.Print("found newline at", offset)
			this.pre = append(this.pre, b[:offset+1]...)
			this.post = append(this.post, b[offset+1:l]...)
		} else { // No newline, so just continue copying
			this.pre = append(this.pre, b...)
		}
		if 0 != len(this.pre) {
			fmt.Printf("last pre char is %d", this.pre[len(this.pre)-1])
		}
		fmt.Print("\n  PRE:", stringMin(this.pre))
		fmt.Print(" POST:", stringMin(this.post), "\n")
		fmt.Print(" SEND:", stringMin(this.tosend), "\n\n")
	} else { // Read more bytes and stash into post
		fmt.Print("pre is full...")
		l, e := this.reader.Read(b)
		fmt.Println("\nreader.Read ", string(b))
		if l <= 0 || e != nil { // If unable to read anything, yield
			n = l
			err = e
			return
		}
		offset := bytes.IndexByte(b[:l], '\n') // Look for a newline
		if 0 <= offset {                       // Found a newline so move post and this to tosend
			this.tosend = this.post
			this.tosend = append(this.tosend, b[:offset+1]...)
			this.post = b[offset+1:]
		} else { // No newline, so just continue copying
			this.post = append(this.post, b...)
		}
		fmt.Print("\n  PRE:", stringMin(this.pre))
		fmt.Print(" POST:", stringMin(this.post), "\n")
		fmt.Print(" SEND:", stringMin(this.tosend), "\n\n")
	}

	return
}

// Field counters

const FieldMax int = 500

func countFields(reader *csv.Reader, header string) {
	var f int
	var counts [FieldMax]int
	var strs []string
	var err error
	reader.FieldsPerRecord = -1

	var startTime time.Time = time.Now()

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

	fmt.Println("Done.", time.Since(startTime))
}

/* Exercises the countFields function on a simple string derived CSV.
 */
func testReadString(str []byte, desc string) {
	var reader *csv.Reader
	reader = csv.NewReader(bytes.NewReader(str))
	countFields(reader, desc)
}

/* Exercises the countFields function on a local CSV file.
 */
func testReadFile() {
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

// Concurrent http file read ////////

var Sources = []string{
	"http://west.xfltr.com.s3-us-west-2.amazonaws.com/census/ss10pusa.csv", // 3.502 Gb
	"http://west.xfltr.com.s3-us-west-2.amazonaws.com/census/ss10pusd.csv", // 2.941 Gb
	"http://west.xfltr.com.s3-us-west-2.amazonaws.com/census/ss10pusc.csv", // 2.806 Gb
	"http://west.xfltr.com.s3-us-west-2.amazonaws.com/census/ss10pusb.csv"} // 2.643 Gb

func recordFlatten(rec [][]byte) (f []byte) {
	f = make([]byte, 0)
	for _, s := range rec {
		f = append(f, s...)
	}
	return
}

var Partials [][]byte

func testSplitWorker(wg *sync.WaitGroup, urlParsed *url.URL, threadId int64, from int64, to int64) {
	defer wg.Done()

	now := time.Now()

	// Prep http request header field

	rng := fmt.Sprintf("bytes=%d-%d", from, (to - 1)) // The http range header which specifies an inclusive range.

	fmt.Printf("%d:: %s %d\n", threadId, rng, to-from)

	// Open connection

	client := &http.Client{}
	resp, err := client.Do(&http.Request{URL: urlParsed, Header: http.Header{"Range": []string{rng}}})
	if err != nil {
		panic(err)
	}

	// Consider response

	defer resp.Body.Close()

	if resp.StatusCode != 206 {
		panic(fmt.Errorf("%d:: non 206: %s\n", threadId, resp))
	}

	serversays := strings.Split(strings.Split(resp.Header["Content-Range"][0], "/")[0], "-")

	fmt.Printf("%d:: serversays:%s\n", threadId, serversays)

	serversays[0] = strings.TrimPrefix(serversays[0], "bytes ")

	if len(serversays) == 2 && serversays[0] == serversays[1] {
		fmt.Printf("%d:: server reports empty\n", threadId)
		return
	}

	filename := fmt.Sprintf("data-%02d", threadId)
	o, err := os.Create(filename)
	defer o.Close()
	if err != nil {
		panic(err)
	}

	var copiedBytes int64 = 0
	//copiedBytes, err = io.Copy(o, resp.Body)

	//csvreader := csv.NewReader(resp.Body)
	//csvreader := csv.NewReader(bufio.NewReader(resp.Body))
	var bufr *bufrecs.BufRecs = bufrecs.NewBufRecs(resp.Body)
	Partials[threadId*2] = bufr.Get()
	csvreader := csv.NewReader(bufr) // Pass the remote file chunk to csv

	csvreader.FieldsPerRecord = -1
	countFields(csvreader, fmt.Sprintf("http_%d", threadId))

	Partials[threadId*2+1] = recordFlatten(bufr.FinalPartial)

	fmt.Printf("%d:: \"%s\"  bytes:%d  duration:%s  len:%s  content-range:%s\n", threadId, filename, copiedBytes, time.Since(now), resp.Header["Content-Length"][0], resp.Header["Content-Range"][0])
}

func testSplit() {
	var e error
	var startTime time.Time = time.Now()
	var threadCount int64 = int64(runtime.NumCPU()) * 2
	var uri string = Sources[3]
	var urlLength, t, from, to, checkCount int64
	var done bool

	runtime.GOMAXPROCS(int(threadCount))

	Partials = make([][]byte, threadCount*2)

	fmt.Printf("URL\t\"%s\"\n", uri)

	urlLength = urlContentLength(uri)
	if urlLength < 0 {
		panic(e)
	}

	splitSize := (urlLength / threadCount)

	fmt.Printf("Content len\t[%d %0.3f Gb]\n", urlLength, float64(urlLength)/(1024.0*1024.0*1024.0))
	fmt.Printf("Split size\t[%d]\n", splitSize)

	waitGroup := sync.WaitGroup{}

	urlParsed, e := url.Parse(uri)
	if e != nil {
		panic(e)
	}

	for t, from, done = 0, 0, false; !done; t, from = t+1, to {
		to = from + splitSize
		if (t == (threadCount - 1)) || (to >= urlLength) {
			to = urlLength
			done = true
		}
		waitGroup.Add(1)
		go testSplitWorker(&waitGroup, urlParsed, t, from, to)
		checkCount += (to - from)
	}

	if checkCount != urlLength {
		panic(errors.New("ERROR!  Sum of separate content-lengths do not equal the original URL's content length"))
	}

	fmt.Println("Waiting on", t, "threads...")
	waitGroup.Wait()

	for _, r := range Partials {
		fmt.Println("Partials:", string(r))
	}

	testReadString(recordFlatten(Partials), "Reassembled interchunk partials.")

	fmt.Println("Done.", time.Since(startTime))
} // testsplit

// Main ////////

func main() {
	//testReadString([]byte("ab,cd,12\nAB,CD,12,34\nx,y,z\n", "static string"))
	//testReadFile()
	testSplit()
}
