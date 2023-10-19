package cmd

import (
	"strconv"
	"time"

	"github.com/nextbillion-ai/gsg/common"
	"github.com/nextbillion-ai/gsg/gcs"
	"github.com/nextbillion-ai/gsg/logger"
	"github.com/nextbillion-ai/gsg/system"

	"github.com/spf13/cobra"
)

func init() {
	rootCmd.AddCommand(lockCmd)
}

var lockCmd = &cobra.Command{
	Use:   "lock destination-gcs-url [ttl(seconds)]",
	Short: "create lock at destination or fail",
	Long:  "create lock at destination or fail",
	Args:  cobra.MinimumNArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		dst := args[0]
		var ttlInSec int
		var ttlErr error
		if len(args) >= 2 {
			if ttlInSec, ttlErr = strconv.Atoi(args[1]); ttlErr != nil {
				logger.Info(module, "invalid ttl")
				common.Exit()
			}
		} else {
			ttlInSec = 24 * 3600
		}
		fo := system.ParseFileObject(dst)
		if fo.System.Scheme() != "gs" {
			logger.Info(module, "only gcs locks are supported")
			common.Exit()
		}
		if fo.FileType() != system.FileType_Directory {
			logger.Info(module, "lock destination is a directory")
			common.Exit()
		}
		gcs := fo.System.(*gcs.GCS)
		pool.Add(func() { gcs.AttemptLock(fo.Bucket, fo.Prefix, time.Duration(int64(time.Second)*int64(ttlInSec))) })
	},
}
