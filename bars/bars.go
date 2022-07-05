package bars

import (
	"github.com/vbauerster/mpb/v7"
	"github.com/vbauerster/mpb/v7/decor"
)

func AddNewDownloadBar(p *mpb.Progress, filename string, size int64) (b *mpb.Bar) {
	p.AddBar(
		size,
		mpb.BarFillerClearOnComplete(),
		mpb.PrependDecorators(
			decor.Name(filename+":", decor.WC{W: len(filename) + 2, C: decor.DidentRight}),
			decor.OnComplete(decor.Name("Downloading", decor.WCSyncSpaceR), "Done!"),
		),
		mpb.AppendDecorators(
			decor.OnComplete(decor.Percentage(decor.WC{W: 5}), ""),
		),
	)
	return
}

func AddNewExtractBar(p *mpb.Progress, file string) (b *mpb.Bar) {
	b = p.Add(
		int64(1),
		mpb.NewBarFiller(
			mpb.SpinnerStyle([]string{"∙∙∙", "●∙∙", "∙●∙", "∙∙●", "∙∙∙"}...).PositionLeft(),
		),
		mpb.BarRemoveOnComplete(),
		mpb.PrependDecorators(
			decor.Name(file+":", decor.WC{W: len(file) + 2, C: decor.DidentRight}),
			decor.OnComplete(decor.Name("Extracting", decor.WCSyncSpaceR), "Done!"),
			decor.OnAbort(decor.Name("Extracting", decor.WCSyncSpaceR), "Failed!"),
		),
	)
	return
}

func AddNewInsertBar(p *mpb.Progress, part string) (b *mpb.Bar) {
	b = p.Add(
		int64(1),
		mpb.NewBarFiller(
			mpb.SpinnerStyle([]string{"∙∙∙", "●∙∙", "∙●∙", "∙∙●", "∙∙∙"}...).PositionLeft(),
		),
		mpb.BarRemoveOnComplete(),
		mpb.PrependDecorators(
			decor.Name("Part "+part+":", decor.WC{W: len(part) + 6, C: decor.DidentRight}),
			decor.OnComplete(decor.Name("Inserting to intermediate", decor.WCSyncSpaceR), "Done!"),
			decor.OnAbort(decor.Name("Inserting to intermediate", decor.WCSyncSpaceR), "Failed!"),
		),
	)
	return
}

func AddNewGoldIngestBar(p *mpb.Progress, day string) (b *mpb.Bar) {
	b = p.Add(
		int64(1),
		mpb.NewBarFiller(
			mpb.SpinnerStyle([]string{"∙∙∙", "●∙∙", "∙●∙", "∙∙●", "∙∙∙"}...).PositionLeft(),
		),
		mpb.BarRemoveOnComplete(),
		mpb.PrependDecorators(
			decor.Name("Day "+day+":", decor.WC{W: 7, C: decor.DidentRight}),
			decor.OnComplete(decor.Name("Inserting day "+day, decor.WCSyncSpaceR), "Done!"),
			decor.OnAbort(decor.Name("Inserting day "+day, decor.WCSyncSpaceR), "Failed!"),
		),
	)
	return
}
