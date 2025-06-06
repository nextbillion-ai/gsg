package cmd

import (
	"fmt"

	"github.com/nextbillion-ai/gsg/common"
	"github.com/nextbillion-ai/gsg/logger"
	"github.com/nextbillion-ai/gsg/system"

	"github.com/spf13/cobra"
)

func init() {
	duCmd.Flags().BoolP("h", "h", false, "print object sizes in human-readable format")
	duCmd.Flags().BoolP("s", "s", false, "print total size only")
	rootCmd.AddCommand(duCmd)
}

var duCmd = &cobra.Command{
	Use:   "du [-sh] [url]",
	Short: "Get disk usage of objects",
	Long:  "Get disk usage objects",
	Args:  cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		isHuman, _ := cmd.Flags().GetBool("h")
		isSum, _ := cmd.Flags().GetBool("s")
		fo := system.ParseFileObject(args[0])
		if fo.FileType() == system.FileType_Invalid {
			logger.Info(module, "Invalid bucket[%s] with prefix[%s]", fo.Bucket, fo.Prefix)
			common.Exit()
		}
		var objs []system.DiskUsage
		var err error
		if objs, err = fo.System.DiskUsage(fo.Bucket, fo.Prefix, true); err != nil {
			common.Exit()
		}
		scheme := ""
		if len(fo.System.Scheme()) > 0 {
			scheme = fmt.Sprintf("%s://", fo.System.Scheme())
		}
		bucket := ""
		if len(fo.Bucket) > 0 {
			bucket = fmt.Sprintf("%s/", fo.Bucket)
		}
		for index, obj := range objs {
			size := fmt.Sprintf("%d", obj.Size)
			if isHuman {
				size = common.FromByteSize(size)
			}
			if !isSum || index == len(objs)-1 {
				logger.Output(fmt.Sprintf("%-10s %s%s%s\n", size, scheme, bucket, obj.Name))
			}
		}
	},
}
