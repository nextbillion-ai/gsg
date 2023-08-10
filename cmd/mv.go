package cmd

import (
	"github.com/nextbillion-ai/gsg/common"
	"github.com/nextbillion-ai/gsg/gcp"
	"github.com/nextbillion-ai/gsg/linux"
	"github.com/nextbillion-ai/gsg/logger"

	"github.com/spf13/cobra"
)

func init() {
	mvCmd.Flags().BoolP("r", "r", false, "move an entire directory tree")
	rootCmd.AddCommand(mvCmd)
}

var mvCmd = &cobra.Command{
	Use:   "mv [-r] [source url] [destination url]",
	Short: "Move files and objects",
	Long:  "Move files and objects",
	Args:  cobra.ExactArgs(2),
	Run: func(cmd *cobra.Command, args []string) {
		isRec, _ := cmd.Flags().GetBool("r")
		srcScheme, srcBucket, srcPrefix := common.ParseURL(args[0])
		dstScheme, dstBucket, dstPrefix := common.ParseURL(args[1])

		switch srcScheme + "-" + dstScheme {
		case "gs-gs":
			if gcp.IsDirectory(srcBucket, srcPrefix) {
				if isRec {
					objs := gcp.ListObjects(srcBucket, srcPrefix, isRec)
					for _, obj := range objs {
						srcPath := obj
						dstPath := common.GetDstPath(srcPrefix, obj, dstPrefix)
						pool.Add(func() { gcp.MoveObject(srcBucket, srcPath, dstBucket, dstPath) })
					}
				} else {
					logger.Info("Omitting bucket[%s] prefix[%s]. (Did you mean to do mv -r?)", srcBucket, srcPrefix)
					common.Exit()
				}
			} else if gcp.IsObject(srcBucket, srcPrefix) {
				if gcp.IsDirectory(dstBucket, dstPrefix) {
					_, name := common.ParseFile(srcPrefix)
					dstPrefix = common.JoinPath(dstPrefix, name)
				}
				pool.Add(func() { gcp.MoveObject(srcBucket, srcPrefix, dstBucket, dstPrefix) })
			} else {
				logger.Info("Invalid bucket[%s] with prefix[%s]", srcBucket, srcPrefix)
				common.Exit()
			}
		case "-":
			if !linux.IsDirectoryOrObject(srcPrefix) {
				logger.Info("Invalid prefix[%s]", srcPrefix)
				common.Exit()
			}
			pool.Add(func() { linux.MoveObject(srcPrefix, dstPrefix) })
		default:
			logger.Info("Not supported yet, please use cp or rsync instead")
		}
	},
}
