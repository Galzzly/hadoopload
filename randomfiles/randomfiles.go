package randomfiles

import (
	crand "crypto/rand"
	"io"
	"math/rand"
	"path"

	"github.com/colinmarc/hdfs/v2"
)

type Options struct {
	// Out    io.Writer
	// Source io.Reader

	FileSize int32
	Depth    int32
	Files    int32
	Width    int32

	RandomFanout bool
}

var FilenameSize = 16
var alphabet = []rune("abcdefghijklmnopqrstuvwxyz0123456789-_")

func WriteRandomFiles(root string, depth int32, opts *Options, client *hdfs.Client) error {
	numFiles := opts.Files

	for i := int32(0); i < numFiles; i++ {
		if e := WriteRandomFile(root, opts, client); e != nil {
			return e
		}
	}

	if depth+1 <= opts.Depth {
		numDirs := opts.Depth
		for i := int32(0); i < numDirs; i++ {
			if e := WriteRandomDir(root, depth+1, opts, client); e != nil {
				return e
			}
		}
	}

	return nil
}

func WriteRandomFile(root string, opts *Options, client *hdfs.Client) error {
	filesize := int64(opts.FileSize)
	n := rand.Intn(FilenameSize-4) + 4
	name := RandomFilename(n)
	filepath := path.Join(root, name)
	f, e := client.Create(filepath)
	if e != nil {
		return e
	}
	if _, e := io.CopyN(f, crand.Reader, filesize); e != nil {
		return e
	}
	return f.Close()
}

func RandomFilename(length int) string {
	b := make([]rune, length)
	for i := range b {
		b[i] = alphabet[rand.Intn(len(alphabet))]
	}
	return string(b)
}

func WriteRandomDir(root string, depth int32, opts *Options, client *hdfs.Client) error {
	if depth > opts.Depth {
		return nil
	}
	n := rand.Intn(FilenameSize-4) + 4
	name := RandomFilename(n)
	root = path.Join(root, name)
	if e := client.MkdirAll(root, 0755); e != nil {
		return e
	}

	return WriteRandomFiles(root, depth, opts, client)
}
