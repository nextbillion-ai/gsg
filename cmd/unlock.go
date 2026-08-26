package cmd

import (
	"github.com/nextbillion-ai/gsg/common"
	"github.com/nextbillion-ai/gsg/gcs"
	"github.com/nextbillion-ai/gsg/linux"
	"github.com/nextbillion-ai/gsg/logger"
	"github.com/nextbillion-ai/gsg/oci"
	"github.com/nextbillion-ai/gsg/s3"
	"github.com/nextbillion-ai/gsg/system"

	"github.com/spf13/cobra"
)

func init() {
	rootCmd.AddCommand(unlockCmd)
}

var unlockCmd = &cobra.Command{
	Use:   "unlock destination-gcs-url",
	Short: "release lock at destination or fail",
	Long:  "release lock at destination or fail",
	Args:  cobra.MinimumNArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		dst := args[0]
		fo := system.ParseFileObject(dst)
		if fo.FileType() != system.FileType_Object {
			logger.Info(module, "lock destination is not an object")
			common.Exit()
		}

		if fo.System.Scheme() == "gs" {
			gcs := fo.System.(*gcs.GCS)
			if e := gcs.AttemptUnLock(fo.Bucket, fo.Prefix); e != nil {
				common.Exit()
			}
			common.Finish()
		}

		if fo.System.Scheme() == "s3" {
			gcs := fo.System.(*s3.S3)
			if e := gcs.AttemptUnLock(fo.Bucket, fo.Prefix); e != nil {
				common.Exit()
			}
			common.Finish()
		}

		if fo.System.Scheme() == "oci" {
			o := fo.System.(*oci.OCI)
			if e := o.AttemptUnLock(fo.Bucket, fo.Prefix); e != nil {
				common.Exit()
			}
			common.Finish()
		}

		if fo.System.Scheme() == "" {
			lnx := fo.System.(*linux.Linux)
			if e := lnx.AttemptUnLock(fo.Bucket, fo.Prefix); e != nil {
				common.Exit()
			}

			common.Finish()
		}

		// Not exiting here reported success for a unlock that never happened:
		// the message went to the log and the command still returned 0, so a
		// caller relying on mutual exclusion got none and no failure either.
		logger.Info(module, "unlock is not supported for scheme [%s]", fo.System.Scheme())
		common.Exit()
	},
}
