package bytequeue

import (
	"fmt"
	"net"
)

/* Byte stream which derives bytes from an internet connection
   .Get        get next char/byte from stream
   .Unget      unget the last char/byte got (one one deep)
   .Token      get back accumulated character string
   .TokenClear clear the accumulated char string
*/

type ByteQueue struct {
	Conn net.Conn // internet connection

	buff         []byte      // Array which buffers all new incoming bytes a slice at a time
	slices       chan []byte // Channel of new incoming byte slices
	current      []byte      // Current dequeued slice bytes are pulled from for the user
	idx          int         // Index into the current dequeued slice
	undo         bool        // The last character read will be read again
	recordReset  bool        // Start recording on next get()
	sliceIdx     int         // Index of current slice to begin recording
	recordSlices [][]byte    // Array of recorded slices
}

const BufferSize int = 64
const QueueMax int = 8

// Initializiation //

func ConnQueueNew() *ByteQueue {
	conn, err := connQueueNewSetupInterweb()
	if nil != err {
		return nil
	}

	this := &ByteQueue{
		Conn:         conn,
		buff:         buffMake(),
		slices:       make(chan []byte, QueueMax),
		current:      []byte("."),
		idx:          0,
		undo:         false,
		recordReset:  true,
		sliceIdx:     0,
		recordSlices: [][]byte(nil),
	}

	connQueueNewLaunchDaemons(this)

	return this
}

func connQueueNewSetupInterweb() (conn net.Conn, err error) {
	conn, err = net.Dial("tcp", "72.14.188.107:7155")
	return
}

func connQueueNewLaunchDaemons(this *ByteQueue) {
	go this._readBytesLoop() // Start internet reader thread
}

// Helpers //

func buffMake() (buff []byte) {
	buff = make([]byte, BufferSize)
	for i, _ := range buff {
		buff[i] = '.'
	}
	return
}

// Methods Private //

/* Always read bytes from the internet and keep track in a channel of byte slices
 */
func (this *ByteQueue) _readBytesLoop() {
	//orig := this.buff // Keep track of entire buffer for debugging
	for {
		n, err := this.Conn.Read(this.buff)
		if nil != err {
			fmt.Println("Error: _readBytesLoop can't Read().")
			return
		}
		this.slices <- this.buff[:n] // Stash read slice into channel
		//fmt.Printf("\n[%s]", orig)   // Debug the dump the entire full buffer

		// Create a new buffer if it's full.  Otherwise adjust the remaining buffer slice.
		if n == len(this.buff) {
			this.buff = buffMake()
			//orig = this.buff
		} else {
			this.buff = this.buff[n:]
		}
	}
}

// Methods Public //

/* Read a byte from the stream.
 */
func (this *ByteQueue) Get() byte {
	if this.undo { // Return last read character
		this.undo = false
	} else if this.idx == (len(this.current) - 1) { // Consider new slice of bytes
		if !this.recordReset { // If recording, stash this slice
			this.recordSlices = append(this.recordSlices, this.current[this.sliceIdx:])
		}
		this.current = <-this.slices // Consider next slice of unread bytes
		this.idx = 0
		this.sliceIdx = 0 // Reset token index.  Continue recording from beginning of this slice.
	} else {
		this.idx++
	}

	if this.recordReset {
		this.recordReset = false
		this.sliceIdx = this.idx // Start recording current slice's bytes from this index
		this.recordSlices = this.recordSlices[0:0]
	}

	return this.current[this.idx] // Here's your byte.  All that for one little byte.
}

func (this *ByteQueue) Token() string {
	var buf []byte
	var offset int = 1
	if this.undo {
		offset = 0
	}
	if !this.recordReset {
		for _, s := range this.recordSlices {
			buf = append(buf, s...)
		}
		buf = append(buf, this.current[this.sliceIdx:this.idx+offset]...)
	}
	return string(buf)
}

/* /Reset the recording of bytes that represent the current scanned token
 */
func (this *ByteQueue) TokenClear() {
	this.recordReset = true
}

/* Undo the last byte read from the stream.
 */
func (this *ByteQueue) Unget() {
	this.undo = true
}
