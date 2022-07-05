package extract

import (
	"compress/gzip"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync/atomic"

	magic "hadoopload/extract/internal"

	"github.com/colinmarc/hdfs/v2"
	"github.com/klauspost/pgzip"
	"github.com/vbauerster/mpb/v7"
)

var readLimit uint32 = 3072

type Extractor interface {
	Extract(filename, dest string, p *mpb.Progress, hdfscli *hdfs.Client) error
}

type File struct {
	os.FileInfo
	Header interface{}
	io.ReadCloser
}

type ReadFakeCloser struct {
	io.Reader
}

type Gz struct {
	CompressionLevel int
}

func (rfc ReadFakeCloser) Close() error { return nil }

func Extract(file, destDir string, p *mpb.Progress, hdfscli *hdfs.Client) error {
	iface, err := GetFormat(file, hdfscli)
	if err != nil {
		return err
	}

	u, _ := iface.(Extractor)
	err = u.Extract(file, destDir, p, hdfscli)
	if err != nil {
		return err
	}
	return nil
}

func (gz *Gz) CheckFormat(filename string, hdfscli *hdfs.Client) error {
	l := atomic.LoadUint32(&readLimit)
	h, err := GetFileHeader(filename, l, hdfscli)
	if err != nil {
		return fmt.Errorf("problem looking at %s", filename)
	}
	mu.Lock()
	defer mu.Unlock()

	var m = newMime("Gzip", magic.Gz)
	if !m.detector(h, l) {
		return fmt.Errorf("%s is not a gzip file", filename)
	}
	return nil
}

func (gz *Gz) Extract(filename, destination string, p *mpb.Progress, hdfscli *hdfs.Client) error {
	f, err := hdfscli.Open(filename)
	if err != nil {
		return err
	}
	defer f.Close()
	fname, err := DetermineFilenameGz(f.Name())
	if err != nil {
		return err
	}
	fileout := path.Join(destination, fname)
	// b := p.Add(
	// 	int64(0),
	// 	mpb.NewBarFiller(
	// 		mpb.SpinnerStyle([]string{"∙∙∙", "●∙∙", "∙●∙", "∙∙●", "∙∙∙"}...).PositionLeft(),
	// 	),
	// 	mpb.BarRemoveOnComplete(),
	// 	mpb.PrependDecorators(
	// 		decor.Name("Extracting "+f.Name()+":", decor.WC{W: len(filename) + 13, C: decor.DidentRight}),
	// 		decor.OnComplete(decor.Name("", decor.WCSyncSpaceR), "Done!"),
	// 		decor.OnAbort(decor.Name("", decor.WCSyncSpaceR), "Failed!"),
	// 	),
	// )
	out, err := hdfscli.Create(fileout)
	if err != nil {
		fmt.Println("HDFS Create")
		// b.Abort(true)
		return err
	}
	defer out.Close()

	r, err := pgzip.NewReader(f)
	if err != nil {
		fmt.Println("Zip Reader")
		// b.Abort(true)
		return err
	}
	defer r.Close()

	_, err = io.Copy(out, r)
	if err != nil {
		// b.Abort(true)
		return err
	}
	// b.SetTotal(1, true)
	// b.Wait()

	return nil
}

func DetermineFilenameGz(f string) (filename string, err error) {
	lower := strings.ToLower(f)
	if !(strings.HasSuffix(lower, ".gz") || strings.HasSuffix(lower, "gzip")) {
		err = fmt.Errorf("%s: filename does not look like gzip", f)
		return
	}
	filename = strings.TrimSuffix(f, filepath.Ext(f))
	return
}

func NewGz() *Gz {
	return &Gz{
		CompressionLevel: gzip.DefaultCompression,
	}
}
