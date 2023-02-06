package cmd

import (
	"gsutil-go/common"
	"gsutil-go/gcp"
	"gsutil-go/linux"
	"gsutil-go/logger"

	"github.com/spf13/cobra"
)

func init() {
	rootCmd.AddCommand(hashCmd)
}

var hashCmd = &cobra.Command{
	Use:   "hash [url]",
	Short: "Get hash value of objects",
	Long:  "Get hash value of objects",
	Args:  cobra.ExactArgs(1),
	Run: func(_ *cobra.Command, args []string) {
		scheme, bucket, prefix := common.ParseURL(args[0])

		switch scheme {
		case "gs":
			attrs := gcp.GetObjectAttributes(bucket, prefix)
			if attrs == nil {
				logger.Info("Invalid bucket[%s] with prefix[%s]", bucket, prefix)
				common.Exit()
			}
			logger.Info("%-20s%d", "Hash (CRC32C):", attrs.CRC32C)
		case "":
			attrs := linux.GetObjectAttributes(prefix)
			if attrs == nil {
				logger.Info("Invalid prefix[%s]", prefix)
				common.Exit()
			}
			logger.Info("%-20s%d", "Hash (CRC32C):", attrs.CRC32C)
		default:
			logger.Info("Not supported yet")
		}
	},
}
