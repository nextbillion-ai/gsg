package cmd

import (
	"strconv"
	"time"

	"github.com/nextbillion-ai/gsg/common"
	"github.com/nextbillion-ai/gsg/gcs"
	"github.com/nextbillion-ai/gsg/linux"
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
		if fo.FileType() == system.FileType_Directory {
			logger.Info(module, "lock destination is a directory")
			common.Exit()
		}

		if fo.System.Scheme() == "gs" {
			gcs := fo.System.(*gcs.GCS)
			if e := gcs.AttemptLock(fo.Bucket, fo.Prefix, time.Duration(int64(time.Second)*int64(ttlInSec))); e != nil {
				common.Exit()
			}
			common.Finish()
		}

		if fo.System.Scheme() == "" {
			lnx := fo.System.(*linux.Linux)
			if e := lnx.AttemptLock(fo.Bucket, fo.Prefix, time.Duration(int64(time.Second)*int64(ttlInSec))); e != nil {
				common.Exit()
			}
			common.Finish()
		}

		logger.Info(module, "lock not suported in scheme %s", fo.System.Scheme())
	},
}
