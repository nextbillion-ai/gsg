package cmd

import (
	"strconv"
	"strings"

	"github.com/nextbillion-ai/gsg/common"
	"github.com/nextbillion-ai/gsg/gcp"
	"github.com/nextbillion-ai/gsg/linux"
	"github.com/nextbillion-ai/gsg/logger"

	"github.com/spf13/cobra"
)

func init() {
	duCmd.Flags().BoolP("h", "h", false, "print object sizes in human-readable format")
	rootCmd.AddCommand(duCmd)
}

var duCmd = &cobra.Command{
	Use:   "du [-h] [url]",
	Short: "Get disk usage of objects",
	Long:  "Get disk usage objects",
	Args:  cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		isHuman, _ := cmd.Flags().GetBool("h")
		scheme, bucket, prefix := common.ParseURL(args[0])

		switch scheme {
		case "gs":
			objs := gcp.GetDiskUsageObjects(bucket, prefix, true)
			if len(objs) == 0 {
				logger.Info("Invalid bucket[%s] with prefix[%s]", bucket, prefix)
				common.Exit()
			}
			shift := 0
			for _, obj := range objs {
				parts := strings.Split(obj, " ")
				if len(parts[0]) > shift {
					shift = len(parts[0])
				}
			}
			for _, obj := range objs {
				parts := strings.Split(obj, " ")
				size := parts[0]
				if isHuman {
					size = common.FromByteSize(size)
				}
				logger.Info("%-"+strconv.Itoa(shift+3)+"s %s://%s/%s", size, scheme, bucket, parts[1])
			}
		case "":
			objs := linux.GetDiskUsageObjects(prefix)
			if len(objs) == 0 {
				logger.Info("Invalid prefix[%s]", prefix)
				common.Exit()
			}
			shift := 0
			for _, obj := range objs {
				parts := strings.Split(obj, "\t")
				if len(parts[0]) > shift {
					shift = len(parts[0])
				}
			}
			for _, obj := range objs {
				parts := strings.Split(obj, "\t")
				size := common.ToByteSize(parts[0])
				if isHuman {
					size = common.FromByteSize(size)
				}
				logger.Info("%-"+strconv.Itoa(shift+3)+"s %s", size, parts[1])
			}
		default:
			logger.Info("Not supported yet")
			common.Exit()
		}
	},
}
