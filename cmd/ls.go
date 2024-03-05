package cmd

import (
	"fmt"
	"strconv"

	"github.com/nextbillion-ai/gsg/common"
	"github.com/nextbillion-ai/gsg/logger"
	"github.com/nextbillion-ai/gsg/system"

	"github.com/spf13/cobra"
)

func init() {
	lsCmd.Flags().BoolP("r", "r", false, "recursively list an entire directory tree")
	lsCmd.Flags().BoolP("l", "l", false, "show date and size")
	lsCmd.Flags().BoolP("h", "h", false, "show size in human readable units")
	rootCmd.AddCommand(lsCmd)
}

var lsCmd = &cobra.Command{
	Use:   "ls [-lhr] [url]",
	Short: "List providers, buckets, or objects",
	Long:  "List providers, buckets, or objects",
	Args:  cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		isRec, _ := cmd.Flags().GetBool("r")
		isHuman, _ := cmd.Flags().GetBool("h")
		isLong, _ := cmd.Flags().GetBool("l")
		fo := system.ParseFileObject(args[0])
		objs := fo.System.List(fo.Bucket, fo.Prefix, isRec)
		if len(objs) == 0 {
			logger.Info(module, "No objects found with bucket[%s] with prefix[%s]", fo.Bucket, fo.Prefix)
			common.Exit()
		}
		outputs := []*output{}
		for _, obj := range objs {
			outputs = append(outputs, build(obj, isHuman, isLong))
		}
		write(outputs)
	},
}

type output struct {
	size string
	date string
	file string
}

func write(outputs []*output) {
	sizeWidth := 0
	for _, o := range outputs {
		sizeLen := len(o.size)
		if sizeWidth < sizeLen {
			sizeWidth = sizeLen
		}
	}
	for _, o := range outputs {
		if sizeWidth > 0 {
			logger.Output(fmt.Sprintf("%"+strconv.Itoa(sizeWidth)+"s %19s %s\n", o.size, o.date, o.file))
		} else {
			logger.Output(o.file + "\n")
		}
	}
}

func build(obj *system.FileObject, isHuman, isLong bool) *output {
	var file string
	if len(obj.System.Scheme()) > 0 {
		file = fmt.Sprintf("%s://%s/%s", obj.System.Scheme(), obj.Bucket, obj.Prefix)
	} else {
		file = obj.Prefix
	}
	switch obj.FileType() {
	case system.FileType_Object:
		if isLong {
			size := fmt.Sprintf("%d", obj.Attributes.Size)
			if isHuman {
				size = common.FromByteSize(size)
			}
			modTime := obj.Attributes.ModTime.Format("2006-01-02T15:04:05Z")
			return &output{size: size, date: modTime, file: file}
		} else {
			return &output{file: file}
		}
	default:
		return &output{file: file}
	}
}
