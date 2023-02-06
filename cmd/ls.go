package cmd

import (
	"gsutil-go/common"
	"gsutil-go/gcp"
	"gsutil-go/linux"
	"gsutil-go/logger"

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
		scheme, bucket, prefix := common.ParseURL(args[0])

		switch scheme {
		case "gs":
			objs := gcp.GetObjectsAttributes(bucket, prefix, isRec)
			if len(objs) == 0 {
				logger.Info("Invalid bucket[%s] with prefix[%s]", bucket, prefix)
				common.Exit()
			}
			for _, obj := range objs {
				if len(obj.Name) > 0 {
					logger.Info("%s://%s/%s", scheme, bucket, obj.Name)
				} else if len(obj.Prefix) > 0 {
					logger.Info("%s://%s/%s", scheme, bucket, obj.Prefix)
				}
			}
		case "":
			objs := linux.ListObjects(prefix, isRec)
			if len(objs) == 0 {
				logger.Info("Invalid prefix[%s]", prefix)
				common.Exit()
			}
			for _, obj := range objs {
				logger.Info(obj)
			}
		default:
			logger.Info("Not supported yet")
			common.Exit()
		}
	},
}
