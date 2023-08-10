package cmd

import (
	"github.com/nextbillion-ai/gsg/common"
	"github.com/nextbillion-ai/gsg/gcp"
	"github.com/nextbillion-ai/gsg/linux"
	"github.com/nextbillion-ai/gsg/logger"

	"cloud.google.com/go/storage"
	"github.com/spf13/cobra"
)

func init() {
	rsyncCmd.Flags().BoolP("r", "r", false, "rsync an entire directory tree")
	rsyncCmd.Flags().BoolP("d", "d", false, "delete objects if not exists")
	rsyncCmd.Flags().BoolP("v", "v", false, "force checksum after command operated, raise error if failed")
	rootCmd.AddCommand(rsyncCmd)
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
		srcScheme, srcBucket, srcPrefix := common.ParseURL(args[0])
		dstScheme, dstBucket, dstPrefix := common.ParseURL(args[1])

		logger.Info("Building synchronization state...")

		switch srcScheme + "-" + dstScheme {
		case "gs-":
			if gcp.IsDirectory(srcBucket, srcPrefix) {
				srcAttrs := gcpAttrsToLinuxAttrs(srcPrefix, gcp.GetObjectsAttributes(srcBucket, srcPrefix, isRec))
				dstAttrs := linux.GetObjectsAttributes(dstPrefix, isRec)
				copyList, deleteList := getCopyAndDeleteLists(srcAttrs, dstAttrs, forceChecksum)
				logger.Info("Starting synchronization...")
				for _, obj := range copyList {
					srcPath := common.JoinPath(srcPrefix, obj.RelativePath)
					dstPath := common.JoinPath(dstPrefix, obj.RelativePath)
					gcp.DownloadObjectWithWorkerPool(srcBucket, srcPath, dstPath, pool, bars, forceChecksum)
				}
				if isDel {
					for _, obj := range deleteList {
						dstPath := obj.FullPath
						pool.Add(func() { linux.DeleteObject(dstPath) })
					}
				}
			} else {
				logger.Info(
					"Invalid bucket[%s] with prefix[%s], arg does not name a directory, bucket, or bucket subdir",
					srcBucket, srcPrefix,
				)
				common.Exit()
			}
		case "-gs":
			if linux.IsDirectory(srcPrefix) {
				srcAttrs := linux.GetObjectsAttributes(srcPrefix, isRec)
				dstAttrs := gcpAttrsToLinuxAttrs(dstPrefix, gcp.GetObjectsAttributes(dstBucket, dstPrefix, isRec))
				copyList, deleteList := getCopyAndDeleteLists(srcAttrs, dstAttrs, forceChecksum)
				logger.Info("Starting synchronization...")
				for _, obj := range copyList {
					srcPath := obj.FullPath
					dstPath := common.JoinPath(dstPrefix, obj.RelativePath)
					pool.Add(func() { gcp.UploadObject(srcPath, dstBucket, dstPath, bars) })
				}
				if isDel {
					for _, obj := range deleteList {
						dstPath := obj.FullPath
						pool.Add(func() { gcp.DeleteObject(dstBucket, dstPath) })
					}
				}
			} else {
				logger.Info("Invalid prefix[%s], arg does not name a directory, bucket, or bucket subdir", srcPrefix)
				common.Exit()
			}
		case "gs-gs":
			if gcp.IsDirectory(srcBucket, srcPrefix) {
				srcAttrs := gcpAttrsToLinuxAttrs(srcPrefix, gcp.GetObjectsAttributes(srcBucket, srcPrefix, isRec))
				dstAttrs := gcpAttrsToLinuxAttrs(dstPrefix, gcp.GetObjectsAttributes(dstBucket, dstPrefix, isRec))
				copyList, deleteList := getCopyAndDeleteLists(srcAttrs, dstAttrs, forceChecksum)
				logger.Info("Starting synchronization...")
				for _, attrs := range copyList {
					srcPath := attrs.FullPath
					dstPath := common.JoinPath(dstPrefix, attrs.RelativePath)
					pool.Add(func() { gcp.CopyObject(srcBucket, srcPath, dstBucket, dstPath) })
				}
				if isDel {
					for _, attrs := range deleteList {
						dstPath := attrs.FullPath
						pool.Add(func() { gcp.DeleteObject(dstBucket, dstPath) })
					}
				}
			} else {
				logger.Info(
					"Invalid bucket[%s] with prefix[%s], arg does not name a directory, bucket, or bucket subdir",
					srcBucket, srcPrefix,
				)
				common.Exit()
			}
		case "-":
			if linux.IsDirectory(srcPrefix) {
				srcAttrs := linux.GetObjectsAttributes(srcPrefix, isRec)
				dstAttrs := linux.GetObjectsAttributes(dstPrefix, isRec)
				copyList, deleteList := getCopyAndDeleteLists(srcAttrs, dstAttrs, forceChecksum)
				logger.Info("Starting synchronization...")
				for _, attrs := range copyList {
					srcPath := attrs.FullPath
					dstPath := common.JoinPath(dstPrefix, attrs.RelativePath)
					pool.Add(func() { linux.CopyObject(srcPath, dstPath) })
				}
				if isDel {
					for _, attrs := range deleteList {
						dstPath := attrs.FullPath
						pool.Add(func() { linux.DeleteObject(dstPath) })
					}
				}
			} else {
				logger.Info("Invalid prefix[%s], arg does not name a directory, bucket, or bucket subdir", srcPrefix)
				common.Exit()
			}
		default:
			logger.Info("Not supported yet")
		}
	},
}

func gcpAttrsToLinuxAttrs(prefix string, attrs []*storage.ObjectAttrs) []*linux.FileAttrs {
	res := []*linux.FileAttrs{}
	for _, attr := range attrs {
		_, name := common.ParseFile(attr.Name)
		res = append(res, &linux.FileAttrs{
			FullPath:     attr.Name,
			RelativePath: common.GetRelativePath(prefix, attr.Name),
			Name:         name,
			Size:         attr.Size,
			CRC32C:       attr.CRC32C,
			ModTime:      gcp.GetFileModificationTime(attr),
		})
	}
	return res
}

func getCopyAndDeleteLists(
	srcAttrs, dstAttrs []*linux.FileAttrs,
	forceChecksum bool,
) (
	copyList, deleteList []*linux.FileAttrs,
) {
	// create srcAttrsMap
	srcAttrsMap := map[string]*linux.FileAttrs{}
	for _, attrs := range srcAttrs {
		srcAttrsMap[attrs.RelativePath] = attrs
	}

	// create dstAttrsMap
	dstAttrsMap := map[string]*linux.FileAttrs{}
	for _, attrs := range dstAttrs {
		dstAttrsMap[attrs.RelativePath] = attrs
	}

	// create copyList
	for _, attrs := range srcAttrs {
		dstAttrs, ok := dstAttrsMap[attrs.RelativePath]
		if ok && attrs.Same(dstAttrs, forceChecksum) {
			continue
		}
		copyList = append(copyList, attrs)
	}

	// create deleteList
	for _, attrs := range dstAttrs {
		if _, ok := srcAttrsMap[attrs.RelativePath]; !ok {
			deleteList = append(deleteList, attrs)
		}
	}
	return
}
