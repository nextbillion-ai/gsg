package cmd

import (
	"gsutil-go/bar"
	"gsutil-go/common"
	"gsutil-go/logger"
	"gsutil-go/worker"
	"os"
	"time"

	"github.com/spf13/cobra"
)

var (
	debugging         bool
	enableMultiThread bool
	multiThread       int
	bars              *bar.Container
	pool              *worker.Pool
)

func init() {
	rootCmd.PersistentFlags().BoolP(
		"m", "m", false,
		"enabel concurrency of execution workers",
	)
	rootCmd.PersistentFlags().IntVarP(
		&multiThread, "c", "c", 256,
		"set concurrency of execution workers, limit from 1 to 1000",
	)
	rootCmd.PersistentFlags().Bool(
		"debug", false,
		"enable debugging mode to print more logs",
	)
	rootCmd.PersistentFlags().BoolVar(
		&bar.Pretty, "pretty", false,
		"enable displaying of progress bars",
	)
	rootCmd.CompletionOptions.DisableDefaultCmd = true
	rootCmd.PersistentFlags().BoolP("help", "", false, "help for gsg")
}

func getMultiThread() int {
	if !enableMultiThread {
		return 1
	}
	limit := 1000
	if multiThread <= 0 {
		return 1
	}
	if multiThread > limit {
		return limit
	}
	return multiThread
}

var rootCmd = &cobra.Command{
	Use:   "gsg",
	Short: "A Golang application that lets you access Cloud Storage from the command line.",
	Long: `You can use gsg (Gsutil Go) to do a wide range of bucket and object management tasks, including:
- Calculating hash value of objects.
- Print the value of objects.
- Uploading, downloading, and deleting objects.
- Listing buckets and objects.
- Moving, copying, and renaming objects.`,
}

// TODO: replace this with proper cobra method
func initFlags() {
	for _, v := range os.Args {
		if v == "-m" {
			enableMultiThread = true
		}
		if v == "--debug" {
			debugging = true
			logger.Debugging = true
		}
	}
}

// Execute executes the root command.
func Execute() error {
	defer common.Recovery()
	defer common.Elapsed("Executed command", time.Now())

	initFlags()

	bars, _ = bar.New()
	screenCols, screenLines := bars.GetScreenDimensions()

	selectedMultiThread := getMultiThread()
	pool = worker.New(getMultiThread(), true)
	pool.Run()

	logger.Debug(
		"enableMultiThread=%t, multiThread=%d, getMultiThread=%d, screenCols=%d, screenLines=%d",
		enableMultiThread, multiThread, selectedMultiThread, screenCols, screenLines,
	)

	err := rootCmd.Execute()
	if err != nil {
		logger.Debug("failed with err %s", err)
	}

	pool.Close()

	// sleep so some async writer could flush
	time.Sleep(time.Millisecond * time.Duration(200))
	return err
}
