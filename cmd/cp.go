package cmd

import (
	"github.com/nextbillion-ai/gsg/common"
	"github.com/nextbillion-ai/gsg/linux"
	"github.com/nextbillion-ai/gsg/logger"
	"github.com/nextbillion-ai/gsg/system"

	"github.com/spf13/cobra"
)

func init() {
	cpCmd.Flags().BoolP("r", "r", false, "copy an entire directory tree")
	cpCmd.Flags().BoolP("v", "v", false, "force checksum after command operated, raise error if failed")
	rootCmd.AddCommand(cpCmd)
}

func upload(src, dst *system.FileObject, forceChecksum, isRec bool) {
	switch src.FileType() {
	case system.FileType_Directory:
		if isRec {
			objs := src.System.List(src.Bucket, src.Prefix, isRec)
			for _, obj := range objs {
				path := obj.Prefix
				dstPath := common.GetDstPath(linux.GetRealPath(src.Prefix), path, dst.Prefix)
				pool.Add(func() { dst.System.Upload(path, dst.Bucket, dstPath, system.RunContext{Bars: bars}) })
			}
		} else {
			logger.Info(module, "Omitting prefix[%s]. (Did you mean to do cp -r?)", src.Prefix)
			common.Exit()
		}
	case system.FileType_Object:
		dstPrefix := dst.Prefix
		if dst.FileType() == system.FileType_Directory {
			_, name := common.ParseFile(src.Prefix)
			dstPrefix = common.JoinPath(dstPrefix, name)
		}
		pool.Add(func() { dst.System.Upload(src.Prefix, dst.Bucket, dstPrefix, system.RunContext{Bars: bars}) })
	case system.FileType_Invalid:
		logger.Info(module, "Invalid prefix[%s]", src.Prefix)
		common.Exit()
	}
}

func download(src, dst *system.FileObject, forceChecksum, isRec bool) {
	switch src.FileType() {
	case system.FileType_Directory:
		if isRec {
			objs := src.System.List(src.Bucket, src.Prefix, isRec)
			for _, obj := range objs {
				dstPath := common.GetDstPath(src.Prefix, obj.Prefix, dst.Prefix)
				src.System.Download(src.Bucket, obj.Prefix, dstPath, forceChecksum, system.RunContext{Bars: bars, Pool: pool})
			}
		} else {
			logger.Info("Omitting bucket[%s] prefix[%s]. (Did you mean to do cp -r?)", src.Bucket, src.Prefix)
			common.Exit()
		}
	case system.FileType_Object:
		dstPrefix := dst.Prefix
		if dst.FileType() == system.FileType_Directory {
			_, name := common.ParseFile(src.Prefix)
			dstPrefix = common.JoinPath(dst.Prefix, name)
		}
		src.System.Download(src.Bucket, src.Prefix, dstPrefix, forceChecksum, system.RunContext{Bars: bars, Pool: pool})
	case system.FileType_Invalid:
		logger.Info(module, "Invalid bucket[%s] with prefix[%s]", src.Bucket, src.Prefix)
		common.Exit()
	}
}

func cloudCopy(src, dst *system.FileObject, forceChecksum, isRec bool) {
	if src.System != dst.System {
		logger.Info(module, "inter cloud copy not supported. [%s] => [%s]", src.Bucket, dst.Bucket)
		common.Exit()
	}
	switch src.FileType() {
	case system.FileType_Directory:
		if !isRec {
			logger.Info(module, "Omitting bucket[%s] prefix[%s]. (Did you mean to do cp -r?)", src.Bucket, src.Prefix)
			common.Exit()
		}
		objs := src.System.List(src.Bucket, src.Prefix, isRec)
		for _, obj := range objs {
			dstPath := common.GetDstPath(src.Prefix, obj.Prefix, dst.Prefix)
			pool.Add(func() { src.System.Copy(src.Bucket, obj.Prefix, dst.Bucket, dstPath) })
		}
	case system.FileType_Object:
		dstPrefix := dst.Prefix
		if dst.FileType() == system.FileType_Directory {
			_, name := common.ParseFile(src.Prefix)
			dstPrefix = common.JoinPath(dst.Prefix, name)
		}
		pool.Add(func() { src.System.Copy(src.Bucket, src.Prefix, dst.Bucket, dstPrefix) })
	case system.FileType_Invalid:
		logger.Info(module, "Invalid bucket[%s] with prefix[%s]", src.Bucket, src.Prefix)
		common.Exit()
	}
}

func localCopy(src, dst *system.FileObject, forceChecksum, recursive bool) {
	if src.FileType() == system.FileType_Invalid {
		logger.Info(module, "Invalid local path: [%s]", src.Prefix)
		common.Exit()
	}
	pool.Add(func() { src.System.Copy(src.Bucket, src.Prefix, dst.Bucket, dst.Prefix) })
}

func doCopy(src, dst *system.FileObject, forceChecksum, isRec bool) {
	if dst.Remote {
		if !src.Remote {
			upload(src, dst, forceChecksum, isRec)
		} else {
			cloudCopy(src, dst, forceChecksum, isRec)
		}
	} else {
		if !src.Remote {
			localCopy(src, dst, forceChecksum, isRec)
		} else {
			download(src, dst, forceChecksum, isRec)
		}
	}
}

var cpCmd = &cobra.Command{
	Use:   "cp [-v] [-r] [source url]... [destination url]",
	Short: "Copy files and objects",
	Long:  "Copy files and objects",
	Args:  cobra.MinimumNArgs(2),
	Run: func(cmd *cobra.Command, args []string) {
		isRec, _ := cmd.Flags().GetBool("r")
		forceChecksum, _ := cmd.Flags().GetBool("v")
		dst := system.ParseFileObject(args[len(args)-1])

		for i := 0; i < len(args)-1; i++ {
			src := system.ParseFileObject(args[i])
			doCopy(src, dst, forceChecksum, isRec)
		}
	},
}
