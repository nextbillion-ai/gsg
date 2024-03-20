package cmd

import (
	"github.com/nextbillion-ai/gsg/common"
	"github.com/nextbillion-ai/gsg/system"

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
		src := system.ParseFileObject(args[0])
		dst := system.ParseFileObject(args[1])
		doCopy(src, dst, true, isRec)
		var err error
		switch src.FileType() {
		case system.FileType_Directory:
			var objs []*system.FileObject
			if objs, err = src.System.List(src.Bucket, src.Prefix, isRec); err != nil {
				common.Exit()
			}
			for _, obj := range objs {
				prefix := obj.Prefix
				pool.Add(func() {
					if e := src.System.Delete(src.Bucket, prefix); e != nil {
						common.Exit()
					}
				})
			}
		case system.FileType_Object:
			if err = src.System.Delete(src.Bucket, src.Prefix); err != nil {
				common.Exit()
			}
		}

	},
}
