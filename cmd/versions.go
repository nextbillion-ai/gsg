package cmd

import (
	"gsutil-go/common"
	"gsutil-go/gcp"
	"gsutil-go/logger"

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
		vs := gcp.ListObjects(versionsBucket, versionsPrefix, true)
		for _, v := range vs {
			_, n := common.ParseFile(v)
			if n != "install.sh" {
				logger.Info("https://%s/%s", versionsBucket, v)
			}
		}
	},
}
