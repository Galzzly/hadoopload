package extract

import (
	"io"

	"github.com/colinmarc/hdfs/v2"
)

func GetFileHeader(file string, l uint32, hdfscli *hdfs.Client) ([]byte, error) {
	f, err := hdfscli.Open(file)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return GetHeader(f, l)
}

func GetHeader(r io.Reader, l uint32) (in []byte, err error) {
	var n int
	in = make([]byte, l)

	n, err = io.ReadFull(r, in)
	if err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
		return nil, err
	}
	in = in[:n]
	return in, nil
}
