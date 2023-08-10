package cmd

import (
	"gsg/common"
	"gsg/gcp"
	"gsg/linux"
	"gsg/logger"

	"github.com/spf13/cobra"
)

func init() {
	cpCmd.Flags().BoolP("r", "r", false, "copy an entire directory tree")
	cpCmd.Flags().BoolP("v", "v", false, "force checksum after command operated, raise error if failed")
	rootCmd.AddCommand(cpCmd)
}

var cpCmd = &cobra.Command{
	Use:   "cp [-v] [-r] [source url]... [destination url]",
	Short: "Copy files and objects",
	Long:  "Copy files and objects",
	Args:  cobra.MinimumNArgs(2),
	Run: func(cmd *cobra.Command, args []string) {
		isRec, _ := cmd.Flags().GetBool("r")
		forceChecksum, _ := cmd.Flags().GetBool("v")
		dstScheme, dstBucket, dstPrefix := common.ParseURL(args[len(args)-1])

		for i := 0; i < len(args)-1; i++ {
			srcScheme, srcBucket, srcPrefix := common.ParseURL(args[i])

			switch srcScheme + "-" + dstScheme {
			case "gs-":
				if gcp.IsDirectory(srcBucket, srcPrefix) {
					if isRec {
						objs := gcp.ListObjects(srcBucket, srcPrefix, isRec)
						for _, obj := range objs {
							dstPath := common.GetDstPath(srcPrefix, obj, dstPrefix)
							gcp.DownloadObjectWithWorkerPool(srcBucket, obj, dstPath, pool, bars, forceChecksum)
						}
					} else {
						logger.Info("Omitting bucket[%s] prefix[%s]. (Did you mean to do cp -r?)", srcBucket, srcPrefix)
						common.Exit()
					}
				} else if gcp.IsObject(srcBucket, srcPrefix) {
					if linux.IsDirectory(dstPrefix) {
						_, name := common.ParseFile(srcPrefix)
						dstPrefix = common.JoinPath(dstPrefix, name)
					}
					gcp.DownloadObjectWithWorkerPool(srcBucket, srcPrefix, dstPrefix, pool, bars, forceChecksum)
				} else {
					logger.Info("Invalid bucket[%s] with prefix[%s]", srcBucket, srcPrefix)
					common.Exit()
				}
			case "-gs":
				if linux.IsDirectory(srcPrefix) {
					if isRec {
						objs := linux.ListObjects(srcPrefix, isRec)
						for _, obj := range objs {
							lobj := obj
							dstPath := common.GetDstPath(linux.GetRealPath(srcPrefix), lobj, dstPrefix)
							pool.Add(func() { gcp.UploadObject(lobj, dstBucket, dstPath, bars) })
						}
					} else {
						logger.Info("Omitting prefix[%s]. (Did you mean to do cp -r?)", srcBucket)
						common.Exit()
					}
				} else if linux.IsObject(srcPrefix) {
					if gcp.IsDirectory(dstBucket, dstPrefix) {
						_, name := common.ParseFile(srcPrefix)
						dstPrefix = common.JoinPath(dstPrefix, name)
					}
					pool.Add(func() { gcp.UploadObject(srcPrefix, dstBucket, dstPrefix, bars) })
				} else {
					logger.Info("Invalid prefix[%s]", srcPrefix)
					common.Exit()
				}
			case "gs-gs":
				if gcp.IsDirectory(srcBucket, srcPrefix) {
					if isRec {
						objs := gcp.ListObjects(srcBucket, srcPrefix, isRec)
						for _, obj := range objs {
							srcPath := obj
							dstPath := common.GetDstPath(srcPrefix, obj, dstPrefix)
							pool.Add(func() { gcp.CopyObject(srcBucket, srcPath, dstBucket, dstPath) })
						}
					} else {
						logger.Info("Omitting bucket[%s] prefix[%s]. (Did you mean to do cp -r?)", srcBucket, srcPrefix)
						common.Exit()
					}
				} else if gcp.IsObject(srcBucket, srcPrefix) {
					if gcp.IsDirectory(dstBucket, dstPrefix) {
						_, name := common.ParseFile(srcPrefix)
						dstPrefix = common.JoinPath(dstPrefix, name)
					}
					pool.Add(func() { gcp.CopyObject(srcBucket, srcPrefix, dstBucket, dstPrefix) })
				} else {
					logger.Info("Invalid bucket[%s] with prefix[%s]", srcBucket, srcPrefix)
					common.Exit()
				}
			case "-":
				if !linux.IsDirectoryOrObject(srcPrefix) {
					logger.Info("Invalid prefix[%s]", srcPrefix)
					common.Exit()
				}
				pool.Add(func() { linux.CopyObject(srcPrefix, dstPrefix) })
			default:
				logger.Info("Not supported yet")
			}
		}
	},
}
