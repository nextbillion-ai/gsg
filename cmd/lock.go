package cmd

import (
	"gsutil-go/common"
	"gsutil-go/gcp"
	"gsutil-go/logger"
	"strconv"
	"time"

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
				logger.Info("invalid ttl")
				common.Exit()
			}
		} else {
			ttlInSec = 24 * 3600
		}
		dstScheme, dstBucket, dstPrefix := common.ParseURL(dst)
		if dstScheme != "gs" {
			logger.Info("only gcs locks are supported")
			common.Exit()
		}
		if gcp.IsDirectory(dstBucket, dstPrefix) {
			logger.Info("lock destination is a directory")
			common.Exit()
		}
		pool.Add(func() { gcp.AttemptLock(dstBucket, dstPrefix, time.Duration(int64(time.Second)*int64(ttlInSec))) })
	},
}
