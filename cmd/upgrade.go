package cmd

import (
	"fmt"
	"runtime"

	"github.com/nextbillion-ai/gsg/common"
	"github.com/nextbillion-ai/gsg/gcp"
	"github.com/nextbillion-ai/gsg/linux"
	"github.com/nextbillion-ai/gsg/logger"

	"github.com/spf13/cobra"
)

const (
	upgradeBucket   = "static.nextbillion.io"
	upgradePrefix   = "tools/gsg"
	upgradeName     = "gsg"
	upgradeVersions = "latest"
)

func init() {
	rootCmd.AddCommand(upgradeCmd)
}

var upgradeCmd = &cobra.Command{
	Use:   "upgrade",
	Short: "Upgrade gsg to the latest version",
	Long:  "Upgrade gsg to the latest version",
	Run: func(_ *cobra.Command, _ []string) {
		// get hash of latest version
		os := runtime.GOOS
		arch := runtime.GOARCH
		srcPath := common.JoinPath(
			upgradePrefix,
			"latest",
			fmt.Sprintf("%s-%s", os, arch),
			upgradeName,
		)
		srcObj := gcp.GetObjectAttributes(upgradeBucket, srcPath)
		if srcObj == nil {
			logger.Info("Not found file: %s", srcPath)
			common.Exit()
		}

		// get hash of current version
		dstPath := common.JoinPath(
			common.GetWorkDir(),
			upgradeName,
		)
		dstObj := linux.GetObjectAttributes(dstPath)
		if dstObj == nil {
			logger.Info("Not found file: %s", dstPath)
			common.Exit()
		}

		// check version
		if dstObj.CRC32C == srcObj.CRC32C {
			logger.Info("Already the latest version")
			return
		}

		// upgrade local version
		gcp.DownloadObject(upgradeBucket, srcPath, dstPath, bars, true)
		common.Chmod(dstPath, 0766)
	},
}
