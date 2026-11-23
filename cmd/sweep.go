package cmd

import (
	"context"
	"fmt"
	"time"

	"github.com/nextbillion-ai/gsg/common"
	"github.com/nextbillion-ai/gsg/gcs"
	"github.com/nextbillion-ai/gsg/logger"
	"github.com/nextbillion-ai/gsg/system"

	"github.com/spf13/cobra"
)

// how long one sweep may take, listing and deletes together
var sweepTimeout = 30 * time.Minute

func init() {
	sweepPartsCmd.Flags().Duration("older-than", gcs.DefaultStalePartAge,
		"take an upload for dead once its newest part is this old")
	rootCmd.AddCommand(sweepPartsCmd)
}

var sweepPartsCmd = &cobra.Command{
	Use:   "sweep-parts [--older-than 1h] gs://bucket/prefix...",
	Short: "Remove the parts that killed composite uploads left behind",
	Long: `Remove the .gsg-part-* (and jam-core's .jam-part-*) objects that composite
uploads under each prefix left behind when their process was killed. An upload
counts as dead only when its newest part is older than --older-than, so an
upload still running is left alone while it finishes a part within every
--older-than; one whose parts each take longer can be swept, and then fails at
its compose. Each part is deleted at the generation listed. Only gs:// is supported: s3 and oci upload in native
multipart uploads, which leave no objects.`,
	Args: cobra.MinimumNArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		olderThan, _ := cmd.Flags().GetDuration("older-than")
		var failed bool
		for _, arg := range args {
			fo := system.ParseFileObject(arg)
			var g *gcs.GCS
			if fo != nil && fo.Remote {
				g, _ = fo.System.(*gcs.GCS)
			}
			if g == nil {
				logger.Info(module, "sweep-parts supports gs:// only, not [%s]", arg)
				common.Exit()
			}
			ctx, cancel := context.WithTimeout(context.Background(), sweepTimeout)
			res, err := g.SweepParts(ctx, fo.Bucket, fo.Prefix, olderThan)
			cancel()
			if err != nil {
				common.ExitWith(err)
			}
			for _, name := range res.Removed {
				logger.Info(module, "Removed gs://%s/%s", fo.Bucket, name)
			}
			for _, name := range res.Left {
				logger.Info(module, "Could not remove gs://%s/%s", fo.Bucket, name)
			}
			fmt.Printf("gs://%s/%s: removed %d part(s), %d of uploads that may still be running left alone, %d could not be removed\n",
				fo.Bucket, fo.Prefix, len(res.Removed), len(res.Live), len(res.Left))
			failed = failed || len(res.Left) > 0
		}
		if failed {
			common.Exit()
		}
	},
}
