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

var HistogramSize int = 500

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
	defer f.Close()

	fs, err := f.Stat()
	if err != nil {
		return
	}
	size = fs.Size()
	return
}

/* Status Rendering.  Pulls info from specific objects and renders them to the console
 */
var FileChunks []*filechunk.FileChunk
var BufRecs []*bufrecs.BufRecs
var ChunkLength []int64
var StatusRenderStop chan int

func statusStart(threadCount int) {
	FileChunks = make([]*filechunk.FileChunk, threadCount)
	BufRecs = make([]*bufrecs.BufRecs, threadCount)
	ChunkLength = make([]int64, threadCount)
	StatusRenderStop = make(chan int)
	go StatusRender()
}

func StatusRender() {
	for {
		time.Sleep(2000 * time.Millisecond)
		fmt.Printf("\nBufRecs Status::")
		for id, br := range BufRecs {
			if nil != br {
				if br.Count == ChunkLength[id] {
					fmt.Printf("   DONE ")
				} else {
					fmt.Printf(" %6.2f%%", 100.0*(float64(br.Count)/float64(ChunkLength[id])))
				}
			} else {
				fmt.Printf(" [ N/A ]")
			}
		}
		select {
		case <-StatusRenderStop:
			break
		default:
		}
	}
	StatusRenderStop <- 1 // Report "Yes, I'm done."
}

/* Histogram helpers
 */
func dumpHistogram(histogram []int64) {
	first := true
	fmt.Print("\nHistogram {")

	if 0 == len(histogram) {
		fmt.Printf("empty")
	}

	for f := 0; f < len(histogram); f++ {
		if 0 < histogram[f] {
			if first {
				first = false
			} else {
				fmt.Print(" ")
			}
			fmt.Printf("%d:%d", f, histogram[f])
		}
	}

	fmt.Print("}")
}

/* Add all the histogram into the first
 */
func histogramsMerge(histograms [][]int64) {
	var histogram0 []int64
	for i, h := range histograms {
		if 0 == i {
			histogram0 = h
			continue
		}
		for i, v := range h {
			if 0 < v {
				histogram0[i] += v
			}
		}
	}
}

/* Given a CSV reader, populate a histogram table of the field counts
 */
func countFields(csvReader *csv.Reader, histogram []int64) {
	histogramLen := len(histogram)
	csvReader.FieldsPerRecord = -1 // Tell the CVS reader to expect an unknown field count
	for {
		strs, err := csvReader.Read()
		if nil != err {
			break
		}
		f := len(strs)
		if f < histogramLen {
			if 0 < f {
				histogram[f]++
			} // There's no such thing as a 0 length field record.
		} else {
			fmt.Print("\nWARNING:", histogramLen, "<", f, "histogram length.")
		}
	}
	return
}

/* Tabulate the field counts of a CSV in string form.
 */
func countCSVFieldsFromString(str []byte, histogram []int64) {
	countFields(csv.NewReader(bytes.NewReader(str)), histogram)
}

/* Exercises the countFields function on a chunked ReadCloser
 */
func countCSVFieldsFromChunkedReaderWorker(wg *sync.WaitGroup, threadId int, r io.ReadCloser, partials [][]byte, histogram []int64) {
	now := time.Now()
	bufr := bufrecs.NewBufRecs(r, threadId) // Create the buffered record reader

	BufRecs[threadId] = bufr // Keep track of the bufrec for status updates

	partials[threadId*2] = bufr.Get() // Keep track of the first record which is assumed to be partial (the rest belonging to the previous thread's chunk.

	countFields(csv.NewReader(bufr), histogram) // Count the fields

	r.Close() // Close the ReadCloser.  No longer needed now

	partials[threadId*2+1] = recordFlatten(bufr.FinalPartial) // Keep track of the last record, which is assumed to be partisl (the rest belonging to the next thread's chunk).

	fmt.Printf("\n%d:: done [%s].", threadId, time.Since(now))
	wg.Done()
}

/* Count the fields per record from a CSV via a local file
 */
func performFileSplitAndCount(threadCount int64, filename string) (histogram []int64) {
	var err error
	var from int64 = 0
	var to int64

	fileSize, err := fileSize(filename)
	if err != nil {
		panic(err)
	}

	lengths := createSplitLengths(fileSize, threadCount)

	wg := sync.WaitGroup{}
	partials := make([][]byte, threadCount*2) // Will contain each thread's pre and post partial records.
	histograms := make([][]int64, threadCount)

	for idx, chunkLength := range lengths {
		ChunkLength[idx] = chunkLength
		wg.Add(1)
		to = from + chunkLength
		f, _ := os.Open(filename) // f is closed in filechunk which is is closed in countCSVFieldsFromChunkedReaderWorker
		fmt.Printf("\nChunk %2d  Range [%10d %10d)  Size %d", idx, from, to, to-from)
		fileChunk := filechunk.NewFileChunk(f, from, to)
		FileChunks[idx] = fileChunk
		histogram := make([]int64, HistogramSize)
		histograms[idx] = histogram
		go countCSVFieldsFromChunkedReaderWorker(&wg, idx, fileChunk, partials, histogram)
		from = to
	}

	if to != fileSize {
		fmt.Printf("\nWARNING:: fileSize %d != %d last chunk range.", to, fileSize)
	}

	wg.Wait()

	countCSVFieldsFromString(recordFlatten(partials), histograms[0]) // Count the inter-thread partials
	histogramsMerge(histograms)
	return histograms[0]
}

/* Count the fields per record from a CSV via HTTP
 */
func performURISplitAndCount(uri string, threadCount int64) (histogram []int64) {
	var e error
	var urlLength, to, from int64

	urlLength = urlContentLength(uri)

	fmt.Printf("\nURL: %s\nContent-length: [%d %0.3f Gb]", uri, urlLength, float64(urlLength)/(1024.0*1024.0*1024.0))

	if urlLength < 0 {
		panic("Content length < 0")
	}

	lengths := createSplitLengths(urlLength, threadCount)
	wg := sync.WaitGroup{}
	from = 0
	urlParsed, e := url.Parse(uri)
	if e != nil {
		panic(e)
	}
	partials := make([][]byte, threadCount*2) // Will contain each thread's pre and post partial records.
	histograms := make([][]int64, threadCount)

	for idx, chunkLength := range lengths {
		ChunkLength[idx] = chunkLength
		wg.Add(1)
		to = from + chunkLength

		rng := fmt.Sprintf("bytes=%d-%d", from, (to - 1)) // The http range header which specifies an inclusive range.

		// Open connection.  resp is a ReadCloser and is closed in countCSVFieldsFromChunkedReaderWorker
		client := &http.Client{}
		resp, err := client.Do(&http.Request{URL: urlParsed, Header: http.Header{"Range": []string{rng}}})
		if err != nil {
			panic(err)
		}

		if resp.StatusCode != 206 { // Consider response
			panic(fmt.Errorf("%d:: non 206: %s\n", idx, resp))
		}

		serversays := strings.Split(strings.Split(resp.Header["Content-Range"][0], "/")[0], "-")
		fmt.Printf("\n%d:: serversays:%s", idx, serversays)
		serversays[0] = strings.TrimPrefix(serversays[0], "bytes ")
		if len(serversays) == 2 && serversays[0] == serversays[1] {
			fmt.Printf("\n%d:: server reports empty\n", idx)
			return
		}

		histogram := make([]int64, HistogramSize)
		histograms[idx] = histogram
		go countCSVFieldsFromChunkedReaderWorker(&wg, idx, resp.Body, partials, histogram)

		from = to
	}

	fmt.Print("\nWaiting on ", threadCount, " threads...")
	wg.Wait()

	countCSVFieldsFromString(recordFlatten(partials), histograms[0]) // Count the inter-thread partials
	histogramsMerge(histograms)
	return histograms[0]
}

// Main ////////

var URIs = []string{
	"http://west.xfltr.com.s3-us-west-2.amazonaws.com/census/ss10pusa.csv", // 3.502 Gb
	"http://west.xfltr.com.s3-us-west-2.amazonaws.com/census/ss10pusd.csv", // 2.941 Gb
	"http://west.xfltr.com.s3-us-west-2.amazonaws.com/census/ss10pusc.csv", // 2.806 Gb
	"http://west.xfltr.com.s3-us-west-2.amazonaws.com/census/ss10pusb.csv"} // 2.643 Gb

func main() {
	var now time.Time = time.Now()
	var threadCount int64 = int64(runtime.NumCPU())
	runtime.GOMAXPROCS(int(threadCount))

	statusStart(int(threadCount)) // Enable the status update thread which dumps periodic info about the workers.

	//// Choose a test to perform

	//histogram := make([]int64, HistogramSize); countCSVFieldsFromString([]byte("ab,cd,12\nAB,CD,12,34\nx,y,z\n"), histogram)
	//histogram := performFileSplitAndCount(threadCount, "data.csv")
	//histogram := performFileSplitAndCount(threadCount, "dat")
	//histogram := performFileSplitAndCount(threadCount, "ss10pusb.csv")
	histogram := performURISplitAndCount(URIs[3], threadCount)

	StatusRenderStop <- 2 // Shutdown the status update thread.

	dumpHistogram(histogram)
	fmt.Printf("\nDone [%s].\n", time.Since(now))
}
