package cmd

import (
	"bufio"
	"crypto/md5"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
	"sync"
	"time"

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

func upload(src, dst *system.FileObject, _, isRec bool, wg *sync.WaitGroup) {
	var err error
	switch src.FileType() {
	case system.FileType_Directory:
		if isRec {
			var objs []*system.FileObject
			if objs, err = src.System.List(src.Bucket, src.Prefix, isRec); err != nil {
				common.Exit()
			}
			for _, obj := range objs {
				op := obj.Prefix
				dstPath := common.GetDstPath(linux.GetRealPath(src.Prefix), op, dst.Prefix)
				wg.Add(1)
				pool.Add(func() {
					defer wg.Done()
					if e := dst.System.Upload(op, dst.Bucket, dstPath, system.RunContext{Bars: bars}); e != nil {
						common.Exit()
					}
				})
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
		wg.Add(1)
		pool.Add(func() {
			defer wg.Done()
			if e := dst.System.Upload(src.Prefix, dst.Bucket, dstPrefix, system.RunContext{Bars: bars}); e != nil {
				common.Exit()
			}
		})
	case system.FileType_Invalid:
		logger.Info(module, "Invalid prefix[%s]", src.Prefix)
		common.Exit()
	}
}

func download(src, dst *system.FileObject, forceChecksum, isRec bool, wg *sync.WaitGroup) {
	var err error
	switch src.FileType() {
	case system.FileType_Directory:
		if isRec {
			var objs []*system.FileObject
			if objs, err = src.System.List(src.Bucket, src.Prefix, isRec); err != nil {
				common.Exit()
			}
			for _, obj := range objs {
				dstPath := common.GetDstPath(src.Prefix, obj.Prefix, dst.Prefix)
				srcPath := obj.Prefix
				wg.Add(1)
				pool.Add(func() {
					defer wg.Done()
					if err = src.System.Download(src.Bucket, srcPath, dstPath, forceChecksum, system.RunContext{Bars: bars, Pool: pool, ChunkSize: chunkSize}); err != nil {
						common.Exit()
					}
				})
			}
		} else {
			logger.Info(module, "Omitting bucket[%s] prefix[%s]. (Did you mean to do cp -r?)", src.Bucket, src.Prefix)
			common.Exit()
		}
	case system.FileType_Object:
		dstPrefix := dst.Prefix
		if dst.FileType() == system.FileType_Directory {
			_, name := common.ParseFile(src.Prefix)
			dstPrefix = common.JoinPath(dst.Prefix, name)
		}
		if err = src.System.Download(src.Bucket, src.Prefix, dstPrefix, forceChecksum, system.RunContext{Bars: bars, Pool: pool, ChunkSize: chunkSize}); err != nil {
			common.Exit()
		}
	case system.FileType_Invalid:
		logger.Info(module, "Invalid bucket[%s] with prefix[%s]", src.Bucket, src.Prefix)
		common.Exit()
	}
}

func interCloudCopy(src, dst *system.FileObject, forceChecksum, isRec bool, wg *sync.WaitGroup) {
	var interChange *system.FileObject
	prepareWorkDir := func() {
		var err error
		workDir := fmt.Sprintf("_icw_/%x", md5.Sum([]byte(fmt.Sprintf("%s-%d", dst.Bucket+dst.Prefix, time.Now().UnixNano()))))
		if err = os.MkdirAll(workDir, 0755); err != nil {
			logger.Error("inter-cloud", "failed to create work dir: %s", workDir)
			common.Exit()
		}
		interChange = system.ParseFileObject(workDir)
		if interChange.FileType() != system.FileType_Directory {
			logger.Error("inter-cloud", "failed to parse work dir: %s to directory", workDir)
			common.Exit()
		}
	}
	removeWorkDir := func() {
		var err error
		if !strings.Contains(interChange.Prefix, "_icw_/") {
			logger.Error("inter-cloud", "invalid work dir: %s to cleanup", interChange.Prefix)
			common.Exit()
		}
		if err = exec.Command("rm", "-rf", interChange.Prefix).Run(); err != nil {
			logger.Error("inter-cloud", "failed to cleanup work dir %s : %s", interChange.Prefix, err)
			common.Exit()
		}
	}
	var err error
	switch src.FileType() {
	case system.FileType_Directory:
		if isRec {
			prepareWorkDir()
			var objs []*system.FileObject
			if objs, err = src.System.List(src.Bucket, src.Prefix, isRec); err != nil {
				common.Exit()
			}
			for _, obj := range objs {
				interPath := common.GetDstPath(src.Prefix, obj.Prefix, interChange.Prefix)
				dstPath := common.GetDstPath(linux.GetRealPath(interChange.Prefix), obj.Prefix, dst.Prefix)
				srcPath := obj.Prefix
				wg.Add(1)
				pool.Add(func() {
					var err error
					defer wg.Done()
					if err = src.System.Download(src.Bucket, srcPath, interPath, forceChecksum, system.RunContext{Bars: bars, Pool: pool, ChunkSize: chunkSize}); err != nil {
						common.Exit()
					}
					interFile := system.ParseFileObject(interPath)
					if interFile.FileType() != system.FileType_Object {
						logger.Error("inter-cloud", "failed to parse intermediate file: %s to file object", interPath)
						common.Exit()
					}
					if err = dst.System.Upload(interPath, dst.Bucket, dstPath, system.RunContext{Bars: bars, Pool: pool}); err != nil {
						logger.Error("inter-cloud", "failed to upload intermediate file: %s to %s", interPath, dstPath)
						common.Exit()
					}
					if err := os.Remove(interPath); err != nil {
						logger.Error("inter-cloud", "failed to remove intermediate file: %s", interPath)
						common.Exit()
					}
				})
			}
			removeWorkDir()
		} else {
			logger.Info(module, "Omitting bucket[%s] prefix[%s]. (Did you mean to do cp -r?)", src.Bucket, src.Prefix)
			common.Exit()
		}
	case system.FileType_Object:
		prepareWorkDir()
		dstPrefix := dst.Prefix
		_, name := common.ParseFile(src.Prefix)
		if dst.FileType() == system.FileType_Directory {
			dstPrefix = common.JoinPath(dst.Prefix, name)
		}
		interPath := common.JoinPath(interChange.Prefix, name)
		if err = src.System.Download(src.Bucket, src.Prefix, interPath, forceChecksum, system.RunContext{Bars: bars, Pool: pool, ChunkSize: chunkSize}); err != nil {
			common.Exit()
		}
		interFile := system.ParseFileObject(interPath)
		if interFile.FileType() != system.FileType_Object {
			logger.Error("inter-cloud", "failed to parse intermediate file: %s to file object", interPath)
			common.Exit()
		}
		if err = dst.System.Upload(interPath, dst.Bucket, dstPrefix, system.RunContext{Bars: bars, Pool: pool}); err != nil {
			logger.Error("inter-cloud", "failed to upload intermediate file: %s to %s", interPath, dstPrefix)
			common.Exit()
		}
		if err := os.Remove(interPath); err != nil {
			logger.Error("inter-cloud", "failed to remove intermediate file: %s", interPath)
			common.Exit()
		}
		removeWorkDir()
	case system.FileType_Invalid:
		logger.Info(module, "Invalid bucket[%s] with prefix[%s]", src.Bucket, src.Prefix)
		common.Exit()
	}
}

func cloudCopy(src, dst *system.FileObject, forceCheckum, isRec bool, wg *sync.WaitGroup) {
	if src.System != dst.System {
		interCloudCopy(src, dst, forceCheckum, isRec, wg)
		return
	}
	var err error
	switch src.FileType() {
	case system.FileType_Directory:
		if !isRec {
			logger.Info(module, "Omitting bucket[%s] prefix[%s]. (Did you mean to do cp -r?)", src.Bucket, src.Prefix)
			common.Exit()
		}
		var objs []*system.FileObject
		if objs, err = src.System.List(src.Bucket, src.Prefix, isRec); err != nil {
			common.Exit()
		}
		for _, obj := range objs {
			op := obj.Prefix
			dstPath := common.GetDstPath(src.Prefix, op, dst.Prefix)
			wg.Add(1)
			pool.Add(func() {
				defer wg.Done()
				if e := src.System.Copy(src.Bucket, op, dst.Bucket, dstPath); e != nil {
					common.Exit()
				}
			})
		}
	case system.FileType_Object:
		dstPrefix := dst.Prefix
		if dst.FileType() == system.FileType_Directory {
			_, name := common.ParseFile(src.Prefix)
			dstPrefix = common.JoinPath(dst.Prefix, name)
		}
		wg.Add(1)
		pool.Add(func() {
			defer wg.Done()
			if e := src.System.Copy(src.Bucket, src.Prefix, dst.Bucket, dstPrefix); e != nil {
				common.Exit()
			}
		})
	case system.FileType_Invalid:
		logger.Info(module, "Invalid bucket[%s] with prefix[%s]", src.Bucket, src.Prefix)
		common.Exit()
	}
}

func localCopy(src, dst *system.FileObject, _, _ bool, wg *sync.WaitGroup) {
	if src.FileType() == system.FileType_Invalid {
		logger.Info(module, "Invalid local path: [%s]", src.Prefix)
		common.Exit()
	}
	wg.Add(1)
	pool.Add(func() {
		defer wg.Done()
		if e := src.System.Copy(src.Bucket, src.Prefix, dst.Bucket, dst.Prefix); e != nil {
			common.Exit()
		}
	})
}

func doCopy(src, dst *system.FileObject, forceChecksum, isRec bool) {
	var wg sync.WaitGroup
	if dst.Remote {
		if !src.Remote {
			upload(src, dst, forceChecksum, isRec, &wg)
		} else {
			cloudCopy(src, dst, forceChecksum, isRec, &wg)
		}
	} else {
		if !src.Remote {
			localCopy(src, dst, forceChecksum, isRec, &wg)
		} else {
			download(src, dst, forceChecksum, isRec, &wg)
		}
	}
	wg.Wait()
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
			src := system.ParseFileObject(parseStdIn(args[i]))
			doCopy(src, dst, forceChecksum, isRec)
		}
	},
}

func parseStdIn(src string) string {
	if src != "-" {
		return src
	}
	reader := bufio.NewReader(os.Stdin)
	var data []byte
	var err error
	if data, err = io.ReadAll(reader); err != nil {
		logger.Error("failed to read stdin for - arg: %s", err.Error())
		common.Exit()
	}
	tmpFile := fmt.Sprintf("/tmp/%d", time.Now().UnixNano())
	if err = os.WriteFile(tmpFile, data, 0600); err != nil {
		logger.Error("failed write tempfile from stdin: %s", err.Error())
		common.Exit()
	}
	return tmpFile
}
