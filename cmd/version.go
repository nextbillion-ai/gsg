package cmd

import (
	"github.com/nextbillion-ai/gsg/logger"

	"github.com/spf13/cobra"
)

var version = "0.0.15"

func init() {
	rootCmd.AddCommand(versionCmd)
}

var versionCmd = &cobra.Command{
	Use:   "version",
	Short: "Print the version number of gsg",
	Long:  "Print the version number of gsg",
	Run: func(_ *cobra.Command, _ []string) {
		logger.Info(module, "gsg version %s", version)
	},
}
