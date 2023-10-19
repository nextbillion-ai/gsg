package cmd

import (
	"fmt"

	"github.com/nextbillion-ai/gsg/common"
	"github.com/nextbillion-ai/gsg/logger"
	"github.com/nextbillion-ai/gsg/system"

	"github.com/spf13/cobra"
)

func init() {
	lsCmd.Flags().BoolP("r", "r", false, "recursively list an entire directory tree")
	rootCmd.AddCommand(lsCmd)
}

var lsCmd = &cobra.Command{
	Use:   "ls [-r] [url]",
	Short: "List providers, buckets, or objects",
	Long:  "List providers, buckets, or objects",
	Args:  cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		isRec, _ := cmd.Flags().GetBool("r")
		fo := system.ParseFileObject(args[0])
		objs := fo.System.List(fo.Bucket, fo.Prefix, isRec)
		if len(objs) == 0 {
			logger.Info("Invalid bucket[%s] with prefix[%s]", fo.Bucket, fo.Prefix)
			common.Exit()
		}
		for _, obj := range objs {
			if len(fo.System.Scheme()) > 0 {
				logger.Output(fmt.Sprintf("%s://%s/%s\n", fo.System.Scheme(), obj.Bucket, obj.Prefix))
			} else {
				logger.Output(fmt.Sprintf("%s\n", obj.Prefix))
			}
		}
	},
}
