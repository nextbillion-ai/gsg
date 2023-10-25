package cmd

import (
	"github.com/nextbillion-ai/gsg/common"
	"github.com/nextbillion-ai/gsg/logger"
	"github.com/nextbillion-ai/gsg/system"

	"github.com/spf13/cobra"
)

func init() {
	rootCmd.AddCommand(hashCmd)
}

var hashCmd = &cobra.Command{
	Use:   "hash [url]",
	Short: "Get checksum value of objects",
	Long:  "Get checksum value of objects",
	Args:  cobra.ExactArgs(1),
	Run: func(_ *cobra.Command, args []string) {
		fo := system.ParseFileObject(args[0])
		attrs := fo.System.Attributes(fo.Bucket, fo.Prefix)
		if attrs == nil {
			logger.Info(module, "Invalid bucket[%s] with prefix[%s]", fo.Bucket, fo.Prefix)
			common.Exit()
		}
		logger.Info("", "%-20s%d", "Hash (CRC32C):", attrs.CRC32)
		logger.Info("", "%-20s%s", "ModTime:", attrs.ModTime.UTC().String())
	},
}
