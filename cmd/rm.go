package cmd

import (
	"gsg/common"
	"gsg/gcp"
	"gsg/linux"
	"gsg/logger"

	"github.com/spf13/cobra"
)

func init() {
	rmCmd.Flags().BoolP("r", "r", false, "remove an entire directory tree")
	rootCmd.AddCommand(rmCmd)
}

var rmCmd = &cobra.Command{
	Use:   "rm [-r] [url]...",
	Short: "Remove objects",
	Long:  "Remove objects",
	Args:  cobra.MinimumNArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		isRec, _ := cmd.Flags().GetBool("r")

		for _, arg := range args {
			scheme, bucket, prefix := common.ParseURL(arg)

			switch scheme {
			case "gs":
				if gcp.IsDirectory(bucket, prefix) {
					if isRec {
						objs := gcp.ListObjects(bucket, prefix, isRec)
						for _, obj := range objs {
							path := obj
							pool.Add(func() { gcp.DeleteObject(bucket, path) })
						}
					} else {
						logger.Info("Omitting bucket[%s] prefix[%s]. (Did you mean to do rm -r?)", bucket, prefix)
						common.Exit()
					}
				} else if gcp.IsObject(bucket, prefix) {
					pool.Add(func() { gcp.DeleteObject(bucket, prefix) })
				} else {
					logger.Info("Invalid bucket[%s] with prefix[%s]", bucket, prefix)
					common.Exit()
				}
			case "":
				if !linux.IsDirectoryOrObject(prefix) {
					logger.Info("Invalid prefix[%s]", prefix)
					common.Exit()
				}
				pool.Add(func() { linux.DeleteObject(prefix) })
			default:
				logger.Info("Not supported yet")
			}
		}
	},
}
