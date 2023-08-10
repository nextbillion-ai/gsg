package cmd

import (
	"github.com/nextbillion-ai/gsg/common"
	"github.com/nextbillion-ai/gsg/gcp"
	"github.com/nextbillion-ai/gsg/logger"

	"github.com/spf13/cobra"
)

func init() {
	rootCmd.AddCommand(unlockCmd)
}

var unlockCmd = &cobra.Command{
	Use:   "unlock destination-gcs-url",
	Short: "release lock at destination or fail",
	Long:  "release lock at destination or fail",
	Args:  cobra.MinimumNArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		dst := args[0]
		dstScheme, dstBucket, dstPrefix := common.ParseURL(dst)
		if dstScheme != "gs" {
			logger.Info("only gcs locks are supported")
			common.Exit()
		}
		if gcp.IsDirectory(dstBucket, dstPrefix) {
			logger.Info("lock destination is a directory")
			common.Exit()
		}
		pool.Add(func() { gcp.AttemptUnLock(dstBucket, dstPrefix) })
	},
}
