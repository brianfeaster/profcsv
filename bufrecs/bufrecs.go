package bufrecs

import (
	"bytes"
	"fmt"
	"io"
)

/* Byte slice queue which derives new slices from a queue of slices and a delimeter.
   .Next   let next complete record (array of byte slices) including the last delimeter.  If no delimeter, it is incomplete and no more records will be produced from the Reader.
*/

type BufRecs struct {
	reader       io.Reader // where bytes will come from
	finalErr     error
	delim        byte          // The delimeter.  Default is ','
	buff         []byte        // Byte array where all new incoming bytes are writen to and then sliced up.  After filling and sliced, a new stage is allocated.
	recStage     [][]byte      // The bytes stage is sliced and queued here until a delimeter is scanned.
	recQueue     chan [][]byte // Channel of records (arrays of byte slices)
	FinalPartial [][]byte
	recOut       [][]byte // Dequeued records that are bled out to Read requests
	err          error
}

const ByteStageSize int = 8192
const QueueMax int = 1024

// Initializiation //

func NewBufRecs(r io.Reader) *BufRecs {
	this := &BufRecs{
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

/* Always read bytes from the Reader into rope
 */
func (this *BufRecs) readBytesLoop() {
	for {
		// Snarf some bytes into our local buffer (which is shifted until full)
		n, err := this.reader.Read(this.buff)

		if n <= 0 {
			fmt.Println("WARNING: readyBytesLoop got nothing.  Expected synchronous behavior.")
		}

		for 0 < n { // Over all bytes...
			di := bytes.IndexByte(this.buff[:n], this.delim) // Find a delimiter
			if di < 0 {                                      // No delimeter [.....n   ]...
				this.recStage = append(this.recStage, this.buff[:n]) // Move slice to stage
				this.buff = this.buff[n:]                            // Shrink buffer to remaining empty bytes
				break
			}
			// A delimeter found [..d...n   ]
			this.recStage = append(this.recStage, this.buff[:di+1]) // Move slice to stage

			// Add the staged record to the channel for human consumption
			this.recQueue <- this.recStage
			this.recStage = make([][]byte, 0, 16)

			this.buff = this.buff[di+1:] // Shrink buffer to remaining empty bytes
			n = n - (di + 1)             // Reduce number of unscanned bytes
		}

		// If the buffer has been used up, create a new one
		if 0 == len(this.buff) {
			this.buff = buffMake()
		}

		if err != nil {
			if 0 < len(this.recStage) {
				this.FinalPartial = this.recStage
			}
			this.finalErr = err
			break
		}
	} // for
	this.recQueue <- nil // Add a sentinel value to the queue
}

// Methods Public //

/* Read a record from the stream
 */
func (this *BufRecs) Get() []byte {
	rec := <-this.recQueue
	if nil == rec {
		return nil
	}
	a := rec[0]
	for _, b := range rec[1:] {
		a = append(a, b...)
	}
	return a
}

func (this *BufRecs) Read(p []byte) (n int, err error) {

	if nil == this.recQueue {
		err = this.finalErr
		return
	}

	for {
		if nil == this.recOut || 0 == len(this.recOut) {
			this.recOut = <-this.recQueue
			if nil == this.recOut {
				this.recQueue = nil
				err = this.finalErr
				return
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
		if 0 == s || n == len(p) {
			break
		} // If nothing copied or p full, return
	}

	return
}
