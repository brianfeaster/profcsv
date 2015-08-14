package filechunk

import (
	"errors"
	"os"
)

type FileChunk struct {
	file      *os.File
	CountLeft int64
}

func NewFileChunk(file *os.File, from int64, to int64) *FileChunk {
	o, err := file.Seek(from, 0)
	if o != from {
		panic("Can't seek")
	}
	if nil != err {
		panic(err)
	}
	return &FileChunk{file: file, CountLeft: to - from}
}

func (this *FileChunk) Read(p []byte) (n int, err error) {
	// When no more bytes shold be read, return nothing and an error.
	if this.CountLeft <= 0 {
		return 0, errors.New("EOChunk")
	}
	// Make sure count bytes are ever read
	if this.CountLeft < int64(len(p)) {
		p = p[:this.CountLeft]
	}
	n, err = this.file.Read(p)
	this.CountLeft = this.CountLeft - int64(n)
	return
}

func (this *FileChunk) Close() error {
	return this.file.Close()
}
