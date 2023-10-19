package cmd

import (
	"github.com/nextbillion-ai/gsg/common"
	"github.com/nextbillion-ai/gsg/logger"
	"github.com/nextbillion-ai/gsg/system"

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
			fo := system.ParseFileObject(arg)
			if fo.FileType() == system.FileType_Invalid {
				logger.Info("Invalid prefix[%s]", fo.Prefix)
				common.Exit()
			}
			switch fo.Remote {
			case true:
				switch fo.FileType() {
				case system.FileType_Directory:
					objs := fo.System.List(fo.Bucket, fo.Prefix, isRec)
					for _, obj := range objs {
						bucket := obj.Bucket
						prefix := obj.Prefix
						pool.Add(func() { fo.System.Delete(bucket, prefix) })
					}
					break
				case system.FileType_Object:
					pool.Add(func() { fo.System.Delete(fo.Bucket, fo.Prefix) })
					break
				}
				break
			case false:
				pool.Add(func() { fo.System.Delete(fo.Bucket, fo.Prefix) })
				break
			}
		}
	},
}
