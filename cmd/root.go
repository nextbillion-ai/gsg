package cmd

import (
	"os"
	"time"

	"github.com/nextbillion-ai/gsg/bar"
	"github.com/nextbillion-ai/gsg/common"
	"github.com/nextbillion-ai/gsg/gcs"
	"github.com/nextbillion-ai/gsg/linux"
	"github.com/nextbillion-ai/gsg/logger"
	"github.com/nextbillion-ai/gsg/r2"
	"github.com/nextbillion-ai/gsg/s3"
	"github.com/nextbillion-ai/gsg/system"
	"github.com/nextbillion-ai/gsg/worker"

	"github.com/spf13/cobra"
)

const (
	module = ""
)

var (
	debugging         bool
	enableMultiThread bool
	mockFail          bool
	multiThread       int
	bars              *bar.Container
	pool              *worker.Pool
)

func init() {
	system.Register(&gcs.GCS{})
	system.Register(&linux.Linux{})
	system.Register(&r2.R2{})
	system.Register(&s3.S3{})
	rootCmd.PersistentFlags().BoolP(
		"m", "m", false,
		"enabel concurrency of execution workers",
	)
	rootCmd.PersistentFlags().IntVarP(
		&multiThread, "c", "c", 64,
		"set concurrency of execution workers, limit from 1 to 1000",
	)
	rootCmd.PersistentFlags().Bool(
		"debug", false,
		"enable debugging mode to print more logs",
	)
	rootCmd.PersistentFlags().Bool(
		"mock-fail", false,
		"enable mocking mode to fail all operations",
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
		if v == "--mock-fail" {
			mockFail = true
		}
	}
}

// Execute executes the root command.
func Execute() error {
	defer common.Recovery()
	defer common.Elapsed("Executed command", time.Now())

	common.AppMode = true
	initFlags()

	bars, _ = bar.New()
	screenCols, screenLines := bars.GetScreenDimensions()

	selectedMultiThread := getMultiThread()
	pool = worker.New(getMultiThread(), true)
	pool.Run()

	logger.Debug(
		module, "enableMultiThread=%t, mockFail=%t, multiThread=%d, getMultiThread=%d, screenCols=%d, screenLines=%d",
		enableMultiThread, mockFail, multiThread, selectedMultiThread, screenCols, screenLines,
	)

	if mockFail {
		common.Exit()
	}

	err := rootCmd.Execute()
	if err != nil {
		logger.Debug(module, "failed with err %s", err)
		common.Exit()
	}

	pool.Close()

	// sleep so some async writer could flush
	time.Sleep(time.Millisecond * time.Duration(200))
	return err
}
