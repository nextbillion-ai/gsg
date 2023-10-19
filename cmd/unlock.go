package cmd

import (
	"github.com/nextbillion-ai/gsg/common"
	"github.com/nextbillion-ai/gsg/gcs"
	"github.com/nextbillion-ai/gsg/logger"
	"github.com/nextbillion-ai/gsg/system"

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
		fo := system.ParseFileObject(dst)
		if fo.System.Scheme() != "gs" {
			logger.Info(module, "only gcs locks are supported")
			common.Exit()
		}
		if fo.FileType() != system.FileType_Object {
			logger.Info(module, "lock destination is not an object")
			common.Exit()
		}
		gcs := fo.System.(*gcs.GCS)
		pool.Add(func() { gcs.AttemptUnLock(fo.Bucket, fo.Prefix) })
	},
}
