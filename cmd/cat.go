package cmd

import (
	"github.com/nextbillion-ai/gsg/common"
	"github.com/nextbillion-ai/gsg/logger"
	"github.com/nextbillion-ai/gsg/system"

	"github.com/spf13/cobra"
)

func init() {
	rootCmd.AddCommand(catCmd)
}

var catCmd = &cobra.Command{
	Use:   "cat [url]",
	Short: "Output the content to stdout",
	Long:  "Output the content to stdout",
	Args:  cobra.ExactArgs(1),
	Run: func(_ *cobra.Command, args []string) {
		fo := system.ParseFileObject(args[0])
		if fo == nil {
			common.Exit()
		}
		var output []byte
		var err error
		if output, err = fo.System.Cat(fo.Bucket, fo.Prefix); err != nil {
			common.Exit()
		}
		logger.Output(string(output))
	},
}
