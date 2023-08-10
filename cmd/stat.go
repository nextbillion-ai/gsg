package cmd

import (
	"encoding/base64"

	"github.com/nextbillion-ai/gsg/common"
	"github.com/nextbillion-ai/gsg/gcp"
	"github.com/nextbillion-ai/gsg/logger"

	"github.com/spf13/cobra"
)

func init() {
	rootCmd.AddCommand(statCmd)
}

var statCmd = &cobra.Command{
	Use:   "stat [url]",
	Short: "Get info of a file or object",
	Long:  "Get info of a file or object",
	Args:  cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		scheme, bucket, prefix := common.ParseURL(args[0])
		switch scheme {
		case "gs":
			obj := gcp.GetObjectAttributes(bucket, prefix)
			if obj == nil {
				logger.Info("Invalid bucket[%s] with prefix[%s]", bucket, prefix)
				common.Exit()
			}
			logger.Info("%s://%s/%s:", scheme, bucket, prefix)
			logger.Info("\t%-s:\t%s", "Creation time", obj.Created)
			logger.Info("\t%-s:\t%s", "Update time", obj.Updated)
			logger.Info("\t%-s:\t%s", "Update time (metadata)", gcp.ParseFileModificationTimeMetadata(obj))
			logger.Info("\t%-s:\t%d", "Content-Length", obj.Size)
			logger.Info("\t%-s:\t%s", "Content-Type", obj.ContentType)
			logger.Info("\t%-s:\t%d", "Hash (crc32c)", obj.CRC32C)
			logger.Info("\t%-s:\t%s", "Hash (md5)", base64.StdEncoding.EncodeToString(obj.MD5))
		default:
			logger.Info("Not supported yet")
			common.Exit()
		}
	},
}
