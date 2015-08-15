package bufrecs

import (
	"bytes"
	"io"
)

/* Byte slice queue which derives new slices from a queue of slices and a delimeter.
   .Next   let next complete record (array of byte slices) including the last delimeter.  If no delimeter, it is incomplete and no more records will be produced from the Reader.
*/

type BufRecs struct {
	id           int
	Count        int64
	reader       io.Reader // where bytes will come from
	finalErr     error
	delim        byte          // The delimeter.  Default is ','
	buff         []byte        // Byte array where all new incoming bytes are writen to and then sliced up.  After filling and sliced, a new stage is allocated.
	recStage     [][]byte      // The bytes stage is sliced and queued here until a delimeter is scanned.
	recQueue     chan [][]byte // Channel of records (arrays of byte slices).  Final record followed by nil.  When no more records, this is set to nil
	FinalPartial [][]byte
	recOut       [][]byte // Dequeued records that are bled out to Read requests
	err          error
}

const ByteStageSize int = 65536
const QueueMax int = 256

// Initializiation //

func NewBufRecs(r io.Reader, id int) *BufRecs {
	this := &BufRecs{
		id:           id,
		Count:        0,
		reader:       r,
		finalErr:     nil,
		delim:        '\n',
		buff:         buffMake(),
		recStage:     make([][]byte, 0, 16),         // Byte slices are queued here
		recQueue:     make(chan [][]byte, QueueMax), // Complete records (array of byte arrays) are queued up here
		FinalPartial: nil,                           // The last partial record, if it exists
		recOut:       nil,
		err:          nil,
	}
	go this.readBytesLoop() // Start byte reader thread
	return this
}

// Helpers //

/* Create a new byte buffer where incoming bytes are spliced from.
 */
func buffMake() (buff []byte) {
	buff = make([]byte, ByteStageSize)
	return
}

// Methods Private //

/* Always read bytes from the Reader into a rope.  A record is a rope consisting of "strings" that end with a delimieter (newline by default).
The last incomplete record, if any, will be kept track of separate from the usual queue.
*/
func (this *BufRecs) readBytesLoop() {
	for {
		// Snarf some bytes into our local buffer (which is shifted until full)
		n, err := this.reader.Read(this.buff)
		this.Count += int64(n)

		for 0 < n { // Over all bytes look for a delimeter...
			di := bytes.IndexByte(this.buff[:n], this.delim) // Find a delimiter
			if di < 0 {                                      // No delimeter found so just add the bytes to the rope [.....n   ]...
				this.recStage = append(this.recStage, this.buff[:n]) // Append slice to the stage (or rope as they say apparently)
				this.buff = this.buff[n:]                            // Shrink buffer to remaining empty bytes
				break
			}

			// A delimeter found [..d...n   ] so add this complete record to the outgoing queue and continue scanning

			this.recStage = append(this.recStage, this.buff[:di+1]) // Move slice to stage

			// Add the staged record to the channel for human consumption
			this.recQueue <- this.recStage
			this.recStage = make([][]byte, 0, 16)

			this.buff = this.buff[di+1:] // Shrink buffer to remaining empty bytes
			n = n - (di + 1)             // Reduce number of unscanned bytes
		}

		if err != nil {
			if 0 < len(this.recStage) {
				this.FinalPartial = this.recStage // The last record is not returned but instead kept track of.  It might be empty
			}
			this.finalErr = err
			break
		}

		if 0 == len(this.buff) { // If the buffer has been used up, create a new one
			this.buff = buffMake()
		}

	} // for

	// Add sentinel values to queue to tell the reader(s) to close itself.
	this.recQueue <- nil
}

// Methods Public //

/* Read a record from the stream
 */
func (this *BufRecs) Get() []byte {
	rec := <-this.recQueue
	if nil == rec { // Close this reader.
		this.recQueue = nil
		return nil
	}
	// Collapse rope into a string
	a := rec[0]
	for _, b := range rec[1:] {
		a = append(a, b...)
	}
	return a
}

func (this *BufRecs) Read(p []byte) (n int, err error) {

	if nil == this.recQueue { // When recQueue is nil, there are no more records.
		err = this.finalErr
		return
	}

	for {
		if nil == this.recOut || 0 == len(this.recOut) { // If recOut hasn't been used yet or it has been read completely...
			this.recOut = <-this.recQueue // Consider a new record from the channel
			if nil == this.recOut {       // Final sentinel has been read.  There are no more records. Close down this reader.
				this.recQueue = nil
				err = this.finalErr
				break
			}
		}

		// Copy as much of the first record's slice as we can
		s := copy(p[n:], this.recOut[0])
		n = n + s
		if s == len(this.recOut[0]) { // If all of the slice was copied, remove it, otherwise shift it
			this.recOut = this.recOut[1:]
		} else {
			this.recOut[0] = this.recOut[0][s:]
		}
		if 0 == s || n == len(p) { // If nothing copied or p full, return
			break
		}
	}

	return
}
