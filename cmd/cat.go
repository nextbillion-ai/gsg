package cmd

import (
	"gsutil-go/common"
	"gsutil-go/gcp"
	"gsutil-go/linux"
	"gsutil-go/logger"

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
		scheme, bucket, prefix := common.ParseURL(args[0])

		switch scheme {
		case "gs":
			obj := gcp.OutputObject(bucket, prefix)
			if obj == nil {
				common.Exit()
			}
			logger.Output(string(obj))
		case "":
			obj := linux.OutputObject(prefix)
			if obj == nil {
				common.Exit()
			}
			logger.Output(string(obj))
		default:
			logger.Info("Not supported yet")
			common.Exit()
		}
	},
}
