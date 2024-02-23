package cmd

import (
	"github.com/nextbillion-ai/gsg/common"
	"github.com/nextbillion-ai/gsg/linux"
	"github.com/nextbillion-ai/gsg/logger"
	"github.com/nextbillion-ai/gsg/system"

	"github.com/spf13/cobra"
)

func init() {
	rsyncCmd.Flags().BoolP("r", "r", false, "rsync an entire directory tree")
	rsyncCmd.Flags().BoolP("d", "d", false, "delete objects if not exists")
	rsyncCmd.Flags().BoolP("v", "v", false, "force checksum after command operated, raise error if failed")
	rootCmd.AddCommand(rsyncCmd)
}
func deleteDst(src, dst *system.FileObject, isRec, isDel, forceChecksum bool) bool {
	if src.FileType() == system.FileType_Invalid && isDel {
		if dst.FileType() == system.FileType_Directory {
			fos := dst.System.List(dst.Bucket, dst.Prefix, true)
			for _, fo := range fos {
				bucket := fo.Bucket
				prefix := fo.Prefix
				system := fo.System
				pool.Add(func() { system.Delete(bucket, prefix) })
			}
		}
		return true
	}
	return false
}

func downsync(src, dst *system.FileObject, isRec, isDel, forceChecksum bool) {
	if deleteDst(src, dst, isRec, isDel, forceChecksum) {
		logger.Debug(module, "cleaned up dst on non-existing src with -d flag")
		return
	}
	deleteTempFiles(dst.Prefix, isRec)
	srcFiles := listRelatively(src, isRec)
	dstFiles := listRelatively(dst, isRec)
	copyList, deleteList := diffs(srcFiles, dstFiles, forceChecksum)
	if len(copyList)+len(deleteList) == 0 {
		logger.Info(module, "No diff detected")
		return
	}
	logger.Info(module, "Starting synchronization...")
	for _, fo := range copyList {
		println(dst.Prefix, fo.Attributes.RelativePath)
		fo.System.Download(fo.Bucket, fo.Prefix, common.JoinPath(dst.Prefix, fo.Attributes.RelativePath), forceChecksum, system.RunContext{Pool: pool, Bars: bars})
	}
	if isDel {
		for _, fo := range deleteList {
			system := fo.System
			bucket := fo.Bucket
			prefix := fo.Prefix
			pool.Add(func() { system.Delete(bucket, prefix) })
		}
	}
}

func upsync(src, dst *system.FileObject, isRec, isDel, forceChecksum bool) {
	if deleteDst(src, dst, isRec, isDel, forceChecksum) {
		logger.Debug(module, "cleaned up dst on non-existing src with -d flag")
		return
	}
	srcFiles := listRelatively(src, isRec)
	dstFiles := listRelatively(dst, isRec)
	copyList, deleteList := diffs(srcFiles, dstFiles, forceChecksum)
	if len(copyList)+len(deleteList) == 0 {
		logger.Info(module, "No diff detected")
		return
	}
	logger.Info(module, "Starting synchronization...")
	for _, fo := range copyList {
		from := fo.Prefix
		dstPath := common.JoinPath(dst.Prefix, fo.Attributes.RelativePath)
		pool.Add(func() { dst.System.Upload(from, dst.Bucket, dstPath, system.RunContext{Bars: bars}) })
	}
	if isDel {
		for _, fo := range deleteList {
			dstPath := common.JoinPath(dst.Prefix, fo.Attributes.RelativePath)
			system := fo.System
			pool.Add(func() { system.Delete(dst.Bucket, dstPath) })
		}
	}
}

func cloudSync(src, dst *system.FileObject, isRec, isDel, forceChecksum bool) {
	if deleteDst(src, dst, isRec, isDel, forceChecksum) {
		logger.Debug(module, "cleaned up dst on non-existing src with -d flag")
		return
	}
	/*
		if src.FileType() == system.FileType_Invalid && isDel {
			if dst.FileType() == system.FileType_Directory {
			}
		}
	*/
	srcFiles := listRelatively(src, isRec)
	dstFiles := listRelatively(dst, isRec)
	copyList, deleteList := diffs(srcFiles, dstFiles, forceChecksum)
	if len(copyList)+len(deleteList) == 0 {
		logger.Info(module, "No diff detected")
		return
	}
	logger.Info(module, "Starting synchronization...")
	for _, fo := range copyList {
		dstPath := common.JoinPath(dst.Prefix, fo.Attributes.RelativePath)
		system := fo.System
		bucket := fo.Bucket
		prefix := fo.Prefix
		pool.Add(func() { system.Copy(bucket, prefix, dst.Bucket, dstPath) })
	}
	if isDel {
		for _, fo := range deleteList {
			dstPath := common.JoinPath(dst.Prefix, fo.Attributes.RelativePath)
			system := fo.System
			pool.Add(func() { system.Delete(dst.Bucket, dstPath) })
		}
	}
}

func localSync(src, dst *system.FileObject, isRec, isDel, forceChecksum bool) {
	if deleteDst(src, dst, isRec, isDel, forceChecksum) {
		logger.Debug(module, "cleaned up dst on non-existing src with -d flag")
		return
	}
	srcFiles := listRelatively(src, isRec)
	dstFiles := listRelatively(dst, isRec)
	copyList, deleteList := diffs(srcFiles, dstFiles, forceChecksum)
	if len(copyList)+len(deleteList) == 0 {
		logger.Info(module, "No diff detected")
		return
	}
	logger.Info(module, "Starting synchronization...")
	for _, fo := range copyList {
		dstPath := common.JoinPath(dst.Prefix, fo.Attributes.RelativePath)
		system := fo.System
		bucket := fo.Bucket
		prefix := fo.Prefix
		pool.Add(func() { system.Copy(bucket, prefix, dst.Bucket, dstPath) })
	}
	if isDel {
		for _, fo := range deleteList {
			dstPath := common.JoinPath(dst.Prefix, fo.Attributes.RelativePath)
			system := fo.System
			pool.Add(func() { system.Delete(dst.Bucket, dstPath) })
		}
	}
}

var rsyncCmd = &cobra.Command{
	Use:   "rsync [-r] [-d] [source url]... [destination url]",
	Short: "Rsync files and objects to destination",
	Long:  "Rsync files and objects to destination",
	Args:  cobra.ExactArgs(2),
	Run: func(cmd *cobra.Command, args []string) {
		isRec, _ := cmd.Flags().GetBool("r")
		isDel, _ := cmd.Flags().GetBool("d")
		forceChecksum, _ := cmd.Flags().GetBool("v")
		src := system.ParseFileObject(args[0])
		dst := system.ParseFileObject(args[1])
		switch src.FileType() {
		case system.FileType_Invalid:
			if !isDel {
				logger.Info(
					module,
					"Invalid bucket[%s] with prefix[%s]",
					src.Bucket, src.Prefix,
				)
				common.Exit()
			}
		case system.FileType_Object:
			logger.Info(
				module,
				"Invalid bucket[%s] with prefix[%s], arg does not name a directory, bucket, or bucket subdir",
				src.Bucket, src.Prefix,
			)
			common.Exit()
		default:
			break
		}

		logger.Info(module, "Building synchronization state...")
		if src.Remote && dst.Remote {
			if src.System.Scheme() != dst.System.Scheme() {
				logger.Info(
					module,
					"rsync from %s to %s is not yet supported",
					src.System.Scheme(), dst.System.Scheme(),
				)
				common.Exit()
			}
			cloudSync(src, dst, isRec, isDel, forceChecksum)
			return
		}
		if src.Remote {
			downsync(src, dst, isRec, isDel, forceChecksum)
			return
		}
		if dst.Remote {
			upsync(src, dst, isRec, isDel, forceChecksum)
			return
		}
		localSync(src, dst, isRec, isDel, forceChecksum)
	},
}

func listRelatively(base *system.FileObject, isRec bool) map[string]*system.FileObject {
	fos := base.System.List(base.Bucket, base.Prefix, isRec)
	r := map[string]*system.FileObject{}
	for _, fo := range fos {
		fo.Attributes.RelativePath = common.GetRelativePath(base.Prefix, fo.Prefix)
		if fo.Attributes.RelativePath == "" {
			continue
		}
		r[fo.Attributes.RelativePath] = fo
	}
	return r
}
func diffs(srcFiles, dstFiles map[string]*system.FileObject, forceChecksum bool) (copyList, deleteList []*system.FileObject) {
	for rp, sf := range srcFiles {
		df, ok := dstFiles[rp]
		if ok && sf.Attributes != nil && sf.Attributes.Same(df.Attributes, forceChecksum) {
			continue
		}
		copyList = append(copyList, sf)
	}
	for rp, df := range dstFiles {
		_, ok := srcFiles[rp]
		if !ok {
			deleteList = append(deleteList, df)
		}
	}
	if debugging && len(copyList) > 0 {
		logger.Debug("diff", "copyList:")
		for _, item := range copyList {
			logger.Debug("diff", "%s", item.Prefix)
		}
	}
	if debugging && len(deleteList) > 0 {
		logger.Debug("diff", "deleteList:")
		for _, item := range deleteList {
			logger.Debug("diff", "%s", item.Prefix)
		}
	}
	return
}

func deleteTempFiles(dir string, isRec bool) {
	objs := linux.ListTempFiles(dir, isRec)
	l := system.Lookup("")
	for _, obj := range objs {
		l.Delete("", obj)
	}
}
