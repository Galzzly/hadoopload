/*
Copyright © 2022 NAME HERE <EMAIL ADDRESS>

*/
package cmd

import (
	"fmt"
	"os"

	nnconnect "hadoopload/hdfs"
	randomfiles "hadoopload/randomfiles"

	"github.com/spf13/cobra"
)

// hdfsCmd represents the hdfs command
var hdfsCmd = &cobra.Command{
	Use:   "hdfs",
	Short: "Write a random directory tree to HDFS path.",
	Long: `Write a random directory tree structure into HDFS, 
	that will be populated with random files.`,
	Run: func(cmd *cobra.Command, args []string) {
		runHdfs()
	},
}

var opts randomfiles.Options
var paths []string

func init() {
	rootCmd.AddCommand(hdfsCmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// hdfsCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// hdfsCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
	hdfsCmd.Flags().Int32VarP(&opts.FileSize, "filesize", "s", 1000000, "The max filesyse to use")
	hdfsCmd.Flags().Int32VarP(&opts.Depth, "depth", "d", 3, "How deep you want the directory tree")
	hdfsCmd.Flags().Int32VarP(&opts.Width, "width", "w", 2, "The number of subdirectories per directory")
	hdfsCmd.Flags().Int32VarP(&opts.Files, "files", "f", 15, "The total number of files")
	hdfsCmd.Flags().StringSliceVarP(&paths, "path", "p", []string{"p1", "p2"}, "Root path(s) to save the directory tree in HDFS (Required)")
	hdfsCmd.MarkFlagRequired("path")
	hdfsCmd.Flags().SortFlags = false
}

func runHdfs() {
	client, err := nnconnect.ConnectToNamenode()
	if err != nil {
		fmt.Fprint(os.Stderr, err)
		return
	}

	for _, root := range paths {
		fmt.Printf("Generating tree for %s ...", root)
		if err := client.MkdirAll(root, 0755); err != nil {
			fmt.Fprintf(os.Stderr, "Error creating directory %s: %s\n", root, err)
			return
		}

		if err := randomfiles.WriteRandomFiles(root, 1, &opts, client); err != nil {
			fmt.Fprint(os.Stderr, err)
			return
		}
		fmt.Println("Done")
	}
}
