package gethivedata

import (
	"io"
	"net/http"

	"github.com/colinmarc/hdfs/v2"
	"github.com/vbauerster/mpb/v7"
)

type WriteCounter struct {
	n   int
	bar *mpb.Bar
}

func (wc *WriteCounter) Write(p []byte) (n int, err error) {
	wc.n += len(p)
	wc.bar.IncrBy(len(p))
	return wc.n, nil
}

func DownloadFile(filename string, target *hdfs.FileWriter, p *mpb.Progress) error {
	// var size int64

	req, err := http.NewRequest(http.MethodGet, "https://placedata.reddit.com/data/canvas-history/"+filename, nil)
	if err != nil {
		return err
	}

	client := &http.Client{
		CheckRedirect: func(r *http.Request, via []*http.Request) error {
			r.URL.Opaque = r.URL.Path
			return nil
		},
	}

	resp, err := client.Do(req)
	if err != nil {
		return err
	}

	defer resp.Body.Close()

	// size, err = strconv.ParseInt(resp.Header["Content-Length"][0], 10, 64)
	// if err != nil {
	// 	return err
	// }

	// b := p.AddBar(
	// 	size,
	// 	mpb.BarFillerClearOnComplete(),
	// 	mpb.PrependDecorators(
	// 		decor.Name("Downloading "+filename+":", decor.WC{W: len(filename) + 14, C: decor.DidentRight}),
	// 		decor.OnComplete(decor.Name("", decor.WCSyncSpaceR), "Done!"),
	// 	),
	// 	mpb.AppendDecorators(
	// 		decor.OnComplete(decor.Percentage(decor.WC{W: 5}), ""),
	// 	),
	// )

	// counter := &WriteCounter{bar: b}
	// _, err = io.Copy(target, io.TeeReader(resp.Body, counter))
	_, err = io.Copy(target, resp.Body)
	if err != nil {
		target.Close()
		return err
	}

	target.Close()
	// b.Wait()
	return nil
}
