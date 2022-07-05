package extract

import (
	magic "hadoopload/extract/internal"
	"sync"
)

var mu = &sync.Mutex{}

type MIME struct {
	filetype string
	detector magic.Detector
}

func newMime(filetype string, detector magic.Detector, children ...*MIME) *MIME {
	return &MIME{filetype, detector}

}
