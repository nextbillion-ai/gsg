package cmd

import (
	"gsutil-go/common"
	"gsutil-go/gcp"
	"gsutil-go/logger"

	"github.com/spf13/cobra"
)

func init() {
	rootCmd.AddCommand(lockCmd)
}

var lockCmd = &cobra.Command{
	Use:   "lock [destination gcs url]",
	Short: "create lock at destination or fail",
	Long:  "create lock at destination or fail",
	Args:  cobra.MinimumNArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		dstScheme, dstBucket, dstPrefix := common.ParseURL(args[len(args)-1])
		if dstScheme != "gs" {
			logger.Info("only gcs locks are supported")
			common.Exit()
		}
		if gcp.IsDirectory(dstBucket, dstPrefix) {
			logger.Info("lock destination is a directory")
			common.Exit()
		}
		pool.Add(func() { gcp.AttemptLock(dstBucket, dstPrefix) })
	},
}
