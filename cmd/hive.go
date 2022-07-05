/*
Copyright © 2022 NAME HERE <EMAIL ADDRESS>

*/
package cmd

import (
	"context"
	"fmt"
	"hadoopload/extract"
	"hadoopload/gethivedata"
	nnconnect "hadoopload/hdfs"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/beltran/gohive"
	"github.com/colinmarc/hdfs/v2"
	"github.com/spf13/cobra"
	"github.com/vbauerster/mpb/v7"
	"github.com/vbauerster/mpb/v7/decor"
)

var (
	hs2host           string
	hs2port           int
	krbbool           bool
	kerberos          string
	stagingdir        string
	numFiles          int
	concurrent        int
	intermediate      bool
	interformat       string
	gold              bool
	goldformat        string
	purgeintermediate bool
)

var (
	barDL      *mpb.Bar
	barExtract *mpb.Bar
	barInter   *mpb.Bar
	barGold    *mpb.Bar
)

var names = []string{"Downloading", "Extracting", "Ingesting to Intermediate", "Ingesting to Gold"}

// hiveCmd represents the hive command
var hiveCmd = &cobra.Command{
	Use:   "hive",
	Short: "Load sample data into Hive.",
	Long: `Making use of the data generated on the Reddit subreddit,
	r/place.`,
	Run: func(cmd *cobra.Command, args []string) {
		runHive()
	},
}

func init() {
	rootCmd.AddCommand(hiveCmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// hiveCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// hiveCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
	hiveCmd.Flags().StringVar(&hs2host, "hs2host", "", "Hostname of your HiveServer2")
	hiveCmd.Flags().IntVar(&hs2port, "hs2port", 10000, "Thrift port of your HiveServer2")
	hiveCmd.Flags().BoolVarP(&krbbool, "kerberos", "k", false, "Whether kerberos is enabled or not")
	hiveCmd.Flags().IntVarP(&numFiles, "numfiles", "n", 79, "The total number of files to ingest")
	hiveCmd.Flags().IntVarP(&concurrent, "staging", "s", 1, "The number of concurrent workers")
	hiveCmd.Flags().StringVarP(&stagingdir, "stagingdir", "d", "", "The staging directory to use")
	hiveCmd.Flags().BoolVarP(&intermediate, "intermediate", "i", false, "Set whether the intermediate table should be managed")
	hiveCmd.Flags().StringVar(&interformat, "intermediate-format", "ORC", "The table format to set the intermediate table, i.e. ORC or PARQUET")
	hiveCmd.Flags().BoolVarP(&gold, "gold", "g", false, "Set whether the gold table should be managed")
	hiveCmd.Flags().StringVar(&goldformat, "gold-format", "ORC", "The table format to set the gold table, i.e. ORC or PARQUET")
	hiveCmd.Flags().BoolVar(&purgeintermediate, "purge", false, "Whether to purge the intermediate table after data is ingested into Gold")
	hiveCmd.Flags().SortFlags = false
}

func runHive() {
	start := time.Now()

	ctx := context.Background()
	configuration := gohive.NewConnectConfiguration()
	configuration.Service = "hive"
	configuration.FetchSize = 1000
	kerberos = "NONE"
	if krbbool {
		kerberos = "KERBEROS"
	}

	connection, err := gohive.Connect(hs2host, hs2port, kerberos, configuration)
	if err != nil {
		fmt.Fprint(os.Stderr, "Error connecting to Hive: ", err)
	}

	cursor := connection.Cursor()

	err = setuptables(ctx, cursor)
	if err != nil {
		fmt.Fprint(os.Stderr, "Error setting up tables: ", err)
		fmt.Println("Completed in", time.Since(start))
		return
	}
	cursor.Close()
	connection.Close()

	// var wg sync.WaitGroup
	// wg.Add(4)
	// var renderDelay chan struct{}
	renderDelay := make(chan struct{})
	// renderDelay <- struct{}{}
	p := mpb.New(mpb.PopCompletedMode(), mpb.WithRenderDelay(renderDelay)) //mpb.WithWaitGroup(&wg)) //, mpb.PopCompletedMode())

	// Build the bars to render
	barDl := p.Add(
		int64(numFiles),
		mpb.NewBarFiller(
			mpb.BarStyle().Lbound("╢").Filler("▌").Tip("▌").Padding("░").Rbound("╟"),
		),
		mpb.BarFillerClearOnComplete(),
		mpb.PrependDecorators(
			decor.Name("Downloading", decor.WC{W: 25}),
		),
		mpb.AppendDecorators(
			decor.OnComplete(decor.CountersNoUnit("%d / %d", decor.WC{W: 7, C: decor.DidentRight}), "Done!"),
		),
	)
	barExtract := p.Add(
		int64(numFiles),
		mpb.NewBarFiller(
			mpb.BarStyle().Lbound("╢").Filler("▌").Tip("▌").Padding("░").Rbound("╟"),
		),
		mpb.BarFillerClearOnComplete(),
		mpb.PrependDecorators(
			decor.Name("Extracting", decor.WC{W: 25}),
		),
		mpb.AppendDecorators(
			decor.OnComplete(decor.CountersNoUnit("%d / %d", decor.WC{W: 7, C: decor.DidentRight}), "Done!"),
		),
	)
	barInter := p.Add(
		int64(numFiles),
		mpb.NewBarFiller(
			mpb.BarStyle().Lbound("╢").Filler("▌").Tip("▌").Padding("░").Rbound("╟"),
		),
		mpb.BarFillerClearOnComplete(),
		mpb.PrependDecorators(
			decor.Name("Ingesting to Intermediate", decor.WC{W: 25}),
		),
		mpb.AppendDecorators(
			decor.OnComplete(decor.CountersNoUnit("%d / %d", decor.WC{W: 7, C: decor.DidentRight}), "Done!"),
		),
	)
	barGold := p.Add(
		int64(5),
		mpb.NewBarFiller(
			mpb.BarStyle().Lbound("╢").Filler("▌").Tip("▌").Padding("░").Rbound("╟"),
		),
		mpb.BarFillerClearOnComplete(),
		mpb.PrependDecorators(
			decor.Name("Ingesting to Gold", decor.WC{W: 25}),
		),
		mpb.AppendDecorators(
			decor.OnComplete(decor.CountersNoUnit("%d / %d", decor.WC{W: 7, C: decor.DidentRight}), "Done!"),
		),
	)
	// <-renderDelay
	close(renderDelay)

	workers := make(chan int, concurrent)

	// Get the hdfs connection
	hdfscli, err := nnconnect.ConnectToNamenode()
	if err != nil {
		barDl.Abort(true)
		barExtract.Abort(true)
		barInter.Abort(true)
		barGold.Abort(true)
		fmt.Fprint(os.Stderr, err)
		fmt.Println("Completed in", time.Since(start))
		return
	}

	// fmt.Println("Ingesting to the intermediate tables...")
	for i := 0; i < numFiles; i++ {
		go ingestintermediate(barDl, barExtract, barInter /*&wg,*/, p, workers, hdfscli, configuration, ctx, i, stagingdir)
	}

	barInter.Wait()
	// fmt.Printf("Intermediate ingestion complete.\n\n")

	// Set up a new set of resources to progress the gold ingestion
	// p = mpb.New(mpb.PopCompletedMode())
	// fmt.Println("Ingesting to Gold table...")
	configuration.HiveConfiguration = map[string]string{"set hive.exec.dynamic.partition.mode": "nonstruct"}
	connection, err = gohive.Connect(hs2host, hs2port, kerberos, configuration)
	if err != nil {
		fmt.Fprint(os.Stderr, "Error connecting to Hive: ", err)
	}
	defer connection.Close()
	cursor = connection.Cursor()
	defer cursor.Close()
	for i := 1; i < 6; i++ {
		err = ingestgold(barGold, p, cursor, ctx, i)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error ingesting into Gold: %s\n", err)
		}
	}
	// p.Wait()
	// fmt.Println("Gold ingestion complete.")
	p.Wait()
	// wg.Wait()
	close(workers)

	if !purgeintermediate {
		fmt.Println("Completed in", time.Since(start))
		return
	}

	fmt.Println("Purging intermediate table")
	cursor.Exec(ctx, "DROP TABLE placedata.placement_data_intermediate")
	if cursor.Err != nil {
		fmt.Fprintf(os.Stderr, "Failed to purge intermediate table: %s\n", cursor.Err)
		fmt.Println("Completed in", time.Since(start))
		return
	}
	fmt.Println("Completed in", time.Since(start))
}

func ingestgold(b *mpb.Bar, p *mpb.Progress, cursor *gohive.Cursor, ctx context.Context, day int) error {
	daystr := strconv.Itoa(day)
	// b := bars.AddNewGoldIngestBar(p, daystr)
	// b := p.Add(
	// 	int64(0),
	// 	mpb.NewBarFiller(
	// 		mpb.SpinnerStyle([]string{"∙∙∙", "●∙∙", "∙●∙", "∙∙●", "∙∙∙"}...).PositionLeft(),
	// 	),
	// 	mpb.BarRemoveOnComplete(),
	// 	mpb.PrependDecorators(
	// 		decor.Name("Inserting Day "+daystr+":", decor.WC{W: 7, C: decor.DidentRight}),
	// 		decor.OnComplete(decor.Name("", decor.WCSyncSpaceR), "Done!"),
	// 		decor.OnAbort(decor.Name("", decor.WCSyncSpaceR), "Failed!"),
	// 	),
	// )

	cursor.Exec(ctx, "INSERT INTO TABLE placedata.placement_data_gold PARTITION (year, month, day, hour) SELECT timeinhour, timezone, user_id, pixel_color, x, y, year as year, month as month, day as day, hour as hour FROM placedata.placement_data_intermediate where day="+daystr)
	if cursor.Err != nil {
		// b.Abort(true)
		return cursor.Err

	}
	// b.SetTotal(1, true)
	b.Increment()

	return nil
}

func ingestintermediate(bDL, bEx, bIn *mpb.Bar /*wg *sync.WaitGroup,*/, p *mpb.Progress, worker chan int, hdfscli *hdfs.Client, configuration *gohive.ConnectConfiguration, ctx context.Context, numDl int, destDir string) {
	// defer wg.Done()
	worker <- 1

	// Set up the hive connection
	connection, err := gohive.Connect(hs2host, hs2port, kerberos, configuration)
	if err != nil {
		fmt.Fprint(os.Stderr, "Error connecting to Hive: ", err)
		<-worker
		return
	}
	defer connection.Close()

	cursor := connection.Cursor()
	defer cursor.Close()

	// Download files into staging dir
	var filename, part string
	part = padpart(numDl)
	filename = "2022_place_canvas_history-0000000000" + part + ".csv.gzip"
	root := filepath.Join(destDir, part)
	target := filepath.Join(destDir, filename)
	// fmt.Println("Creating staging directory", root)
	if err := hdfscli.MkdirAll(root, 0755); err != nil {
		fmt.Printf("Unable to create staging dir %s: %s\n", root, err)
		<-worker
		return
	}
	f, err := hdfscli.Create(target)
	if err != nil {
		fmt.Printf("%s: %s", filename, err)
		<-worker
		return
	}

	// Create the staging table, pointing to the directory created above
	cursor.Exec(ctx, "CREATE EXTERNAL TABLE placedata.placement_data_staging"+part+" (date_time string, user_id string, pixel_color string, coordinate string) ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' WITH SERDEPROPERTIES ('SEPARATORCHAR'=',', 'QUOTECHAR'='\"', 'ESCAPECHAR'='\"') LOCATION '"+stagingdir+"/"+part+"' TBLPROPERTIES('skip.header.line.count'='1')")
	if cursor.Err != nil {
		fmt.Printf("Error creating staging table: %s\n", cursor.Err)
		<-worker
		return
	}

	err = gethivedata.DownloadFile(filename, f, p)
	if err != nil {
		fmt.Printf("%s: %s", filename, err)
		<-worker
		return
	}
	bDL.Increment()

	// extract the file into the staging directory
	err = extract.Extract(target, root, p, hdfscli)
	if err != nil {
		fmt.Printf("%s: %s", filename, err)
		<-worker
		return
	}
	bEx.Increment()

	// Ingest to the intermediate table
	// b := p.New(
	// 	int64(0),
	// 	mpb.BarFillerBuilder(
	// 		mpb.SpinnerStyle([]string{"∙∙∙", "●∙∙", "∙●∙", "∙∙●", "∙∙∙"}...).PositionLeft(),
	// 	),
	// 	mpb.BarRemoveOnComplete(),
	// 	mpb.PrependDecorators(
	// 		decor.Name("Inserting Part "+part+":", decor.WC{W: len(part) + 7, C: decor.DidentRight}),
	// 		decor.OnComplete(decor.Name("", decor.WCSyncSpaceR), "Done!"),
	// 		decor.OnAbort(decor.Name("", decor.WCSyncSpaceR), "Failed!"),
	// 	),
	// )

	cursor.Exec(ctx, "INSERT INTO placedata.placement_data_intermediate SELECT year, month, tbd[0] as day, substr(tbd[1], 0, 2) AS hour, substr(tbd[1], 4, length(tbd[1])) AS timeinhour, tbd[2] AS timezone, user_id, pixel_color, x, y FROM (SELECT dt[0] AS year, dt[1] AS month, split(dt[2], ' ') AS tbd, user_id, pixel_color, x, y FROM (SELECT split(date_time, '-') AS dt, user_id, pixel_color, split(coordinate, ',')[0] AS x, split(coordinate, ',')[1] AS y FROM placedata.placement_data_staging"+part+")s)s")
	if cursor.Err != nil {
		fmt.Printf("Ingest failed for: %s. %s\n", part, cursor.Err)
		// b.Abort(true)
		<-worker
		return
	}
	bIn.Increment()
	// b.SetTotal(1, true)
	// b.Wait()

	// Clean up staging
	err = hdfscli.Remove(target)
	if err != nil {
		fmt.Printf("Failed to remove %s from staging: %s\n", filename, err)
		<-worker
		return
	}

	<-worker
}

func setuptables(ctx context.Context, cursor *gohive.Cursor) error {
	// Create the parent databased
	cursor.Exec(ctx, "CREATE DATABASE placedata")
	if cursor.Err != nil {
		return cursor.Err
	}

	// Create the intermediate table
	if intermediate {
		cursor.Exec(ctx, "CREATE TABLE placedata.placement_data_intermediate (year int, month int, day int, hour int, timeinhour string, timezone string, user_id string, pixel_color string, x int, y int) STORED AS "+interformat)
	} else {
		cursor.Exec(ctx, "CREATE EXTERNAL TABLE placedata.placement_data_intermediate (year int, month int, day int, hour int, timeinhour string, timezone string, user_id string, pixel_color string, x int, y int) STORED AS "+interformat)
	}
	if cursor.Err != nil {
		return cursor.Err
	}

	// Create the Gold table
	if gold {
		cursor.Exec(ctx, "CREATE TABLE placedata.placement_data_gold (timeinhour string, timezone string, user_id string, pixel_color string, x int, y int) PARTITIONED BY (year int, month int, day int, hour int) STORED AS "+goldformat)
	} else {
		cursor.Exec(ctx, "CREATE EXTERNAL TABLE placedata.placement_data_gold (timeinhour string, timezone string, user_id string, pixel_color string, x int, y int) PARTITIONED BY (year int, month int, day int, hour int) STORED AS "+goldformat)
	}
	if cursor.Err != nil {
		return cursor.Err
	}

	return nil
}

func padpart(i int) string {
	if i < 10 {
		return "0" + strconv.Itoa(i)
	}
	return strconv.Itoa(i)
}

func AddBar(p *mpb.Progress, total int64, section string) *mpb.Bar {
	return p.New(
		total,
		mpb.BarFillerBuilder(
			mpb.SpinnerStyle([]string{"∙∙∙", "●∙∙", "∙●∙", "∙∙●", "∙∙∙"}...).PositionLeft(),
		),
		mpb.BarFillerClearOnComplete(),
		mpb.PrependDecorators(
			decor.Name(section, decor.WC{W: len(section) + 1, C: decor.DidentRight}),
		),
		mpb.AppendDecorators(
			decor.OnComplete(decor.CountersNoUnit("% d / % d"), ""),
		),
	)
}
