package cmd

import (
	"strings"

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
		vs := g.List(versionsBucket, versionsPrefix, true)
		for _, v := range vs {
			if !strings.HasSuffix(v.Prefix, "/install.sh") {
				logger.Info(module, "https://%s/%s", versionsBucket, v.Prefix)
			}
		}
	},
}
