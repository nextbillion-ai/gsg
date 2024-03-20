package cmd

import (
	"encoding/base64"

	"cloud.google.com/go/storage"
	"github.com/nextbillion-ai/gsg/common"
	"github.com/nextbillion-ai/gsg/gcs"
	"github.com/nextbillion-ai/gsg/logger"
	"github.com/nextbillion-ai/gsg/system"

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
		fo := system.ParseFileObject(args[0])
		if fo.FileType() != system.FileType_Object {
			logger.Info(module, "Invalid bucket[%s] with prefix[%s]", fo.Bucket, fo.Prefix)
			common.Exit()
		}
		if fo.System.Scheme() != "gs" {
			logger.Info(module, "only gcs is supported")
			common.Exit()
		}
		g := fo.System.(*gcs.GCS)
		var err error
		var attrs *storage.ObjectAttrs
		if attrs, err = g.GCSAttrs(fo.Bucket, fo.Prefix); err != nil {
			common.Exit()
		}
		logger.Info(module, "%s://%s/%s:", fo.System.Scheme(), fo.Bucket, fo.Prefix)
		logger.Info(module, "\t%-s:\t%s", "Creation time", attrs.Created)
		logger.Info(module, "\t%-s:\t%s", "Update time", attrs.Updated)
		logger.Info(module, "\t%-s:\t%s", "Update time (metadata)", gcs.ParseFileModificationTimeMetadata(attrs))
		logger.Info(module, "\t%-s:\t%d", "Content-Length", attrs.Size)
		logger.Info(module, "\t%-s:\t%s", "Content-Type", attrs.ContentType)
		logger.Info(module, "\t%-s:\t%d", "Hash (crc32c)", attrs.CRC32C)
		logger.Info(module, "\t%-s:\t%s", "Hash (md5)", base64.StdEncoding.EncodeToString(attrs.MD5))
	},
}
