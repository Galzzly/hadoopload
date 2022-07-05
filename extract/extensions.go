package extract

import (
	"fmt"

	"github.com/colinmarc/hdfs/v2"
)

type Format interface {
	CheckFormat(filename string, hdfscli *hdfs.Client) error
}

// formats lists the archive formats that we use for now.
// This is by no means extensive, but only what we need
// at the time of writing. More can be added later
var formats = []Format{
	&Gz{},
}

func GetFormat(filename string, hdfscli *hdfs.Client) (interface{}, error) {
	f, err := ByFormat(filename, hdfscli)
	if err != nil {
		return nil, err
	}

	return f, nil
}

func ByFormat(filename string, hdfscli *hdfs.Client) (interface{}, error) {
	var ext interface{}
	for _, c := range formats {
		if err := c.CheckFormat(filename, hdfscli); err == nil {
			ext = c
			break
		}
	}

	switch ext.(type) {
	case *Gz:
		return NewGz(), nil
	}
	return nil, fmt.Errorf("invalid format: %s", filename)
}
