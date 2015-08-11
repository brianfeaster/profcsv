package filechunk

import (
	"errors"
	"os"
)

type FileChunk struct {
	file  *os.File
	count int64
}

func NewFileChunk(file *os.File, offset int64, count int64) *FileChunk {
	o, err := file.Seek(offset, 0)
	if o != offset {
		panic("Can't seek")
	}
	if nil != err {
		panic(err)
	}
	return &FileChunk{file: file, count: count}
}

func (this *FileChunk) Read(p []byte) (n int, err error) {
	// When no more bytes shold be read, return nothing and an error.
	if 0 == this.count {
		return 0, errors.New("End of chunk.")
	}
	// Make sure count bytes are ever read
	if this.count < int64(len(p)) {
		p = p[:this.count]
	}
	n, err = this.file.Read(p)
	this.count = this.count - int64(n)
	return
}

func (this *FileChunk) Close() error {
	return this.file.Close()
}
