package cmd

import (
	"fmt"
	"runtime"

	"github.com/nextbillion-ai/gsg/common"
	"github.com/nextbillion-ai/gsg/logger"
	"github.com/nextbillion-ai/gsg/system"

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
		g := system.Lookup("gs")
		var err error
		var srcObj *system.Attrs
		if srcObj, err = g.Attributes(upgradeBucket, srcPath); err != nil {
			common.Exit()
		}
		if srcObj == nil {
			logger.Info(module, "gsg release not found: %s", srcPath)
			common.Exit()
		}

		// get hash of current version
		dstPath := common.JoinPath(
			common.GetWorkDir(),
			upgradeName,
		)
		l := system.Lookup("")
		var dstObj *system.Attrs
		if dstObj, err = l.Attributes("", dstPath); err != nil {
			common.Exit()
			return
		}
		if dstObj == nil {
			logger.Info(module, "File not found: %s", dstPath)
			common.Exit()
			return
		}

		// check version
		if dstObj.CRC32 == srcObj.CRC32 {
			logger.Info(module, "Already the latest version")
			return
		}

		// upgrade local version
		if err = g.Download(upgradeBucket, srcPath, srcPath, true, system.RunContext{Bars: bars, Pool: pool, ChunkSize: chunkSize}); err != nil {
			common.Exit()
		}
		common.Chmod(dstPath, 0766)
	},
}
