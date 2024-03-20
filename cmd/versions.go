package cmd

import (
	"strings"

	"github.com/nextbillion-ai/gsg/common"
	"github.com/nextbillion-ai/gsg/logger"
	"github.com/nextbillion-ai/gsg/system"

	"github.com/spf13/cobra"
)

const (
	versionsBucket = "static.nextbillion.io"
	versionsPrefix = "tools/gsg"
)

func init() {
	rootCmd.AddCommand(versionsCmd)
}

var versionsCmd = &cobra.Command{
	Use:   "versions",
	Short: "List the versions of gsg",
	Long:  "List the versions of gsg",
	Run: func(_ *cobra.Command, _ []string) {
		g := system.Lookup("gs")
		var err error
		var vs []*system.FileObject
		if vs, err = g.List(versionsBucket, versionsPrefix, true); err != nil {
			common.Exit()
		}
		for _, v := range vs {
			if !strings.HasSuffix(v.Prefix, "/install.sh") {
				logger.Info(module, "https://%s/%s", versionsBucket, v.Prefix)
			}
		}
	},
}
