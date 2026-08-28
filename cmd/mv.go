package cmd

import (
	"strings"

	"github.com/nextbillion-ai/gsg/common"
	"github.com/nextbillion-ai/gsg/logger"
	"github.com/nextbillion-ai/gsg/system"

	"github.com/spf13/cobra"
)

func init() {
	mvCmd.Flags().BoolP("r", "r", false, "move an entire directory tree")
	rootCmd.AddCommand(mvCmd)
}

var mvCmd = &cobra.Command{
	Use:   "mv [-r] [source url] [destination url]",
	Short: "Move files and objects",
	Long:  "Move files and objects",
	Args:  cobra.ExactArgs(2),
	Run: func(cmd *cobra.Command, args []string) {
		isRec, _ := cmd.Flags().GetBool("r")
		src := system.ParseFileObject(args[0])
		dst := system.ParseFileObject(args[1])

		// Moving something onto itself is refused, as gsutil refuses it:
		// `mv: "gs://b/k" and "gs://b/k" are the same file - abort`.
		//
		// mv is a copy followed by a delete of the source, so without this the
		// delete runs whatever the copy did. Whether the object survived came
		// down to whether that backend's Copy happened to fail: measured, s3
		// escaped only because AWS rejects a copy onto itself, and on gs the
		// object was simply gone.
		//
		// Only the exact same path, which is where gsutil draws the line too.
		// A destination inside the source is a real move, and gsutil performs
		// it -- `mv -r d d/sub` leaves d/sub/d/... with the originals gone.
		// The ordering below is what lets this do the same.
		if isSamePath(src, dst) {
			logger.Info(module, "%s and %s are the same object - abort", args[0], args[1])
			common.Exit()
		}

		// What to delete is decided before the copy, not after.
		//
		// Listing afterwards is wrong whenever the destination lives inside
		// the source: the listing then returns the freshly written copies as
		// well, and deleting those throws away the data the move just
		// produced. Measured on gs, `mv -r d d/sub` took two objects to none,
		// and so did `mv -r d d/` -- a trailing slash is one keystroke.
		//
		// Listing first names only what was there to begin with, which is
		// exactly what a move should remove.
		var toDelete []string
		isDir := src.FileType() == system.FileType_Directory
		if isDir {
			var objs []*system.FileObject
			var lerr error
			if objs, lerr = src.System.List(src.Bucket, src.Prefix, isRec); lerr != nil {
				common.Exit()
			}
			for _, obj := range objs {
				toDelete = append(toDelete, obj.Prefix)
			}
		}

		doCopy(src, dst, true, isRec)

		if isDir {
			for _, prefix := range toDelete {
				p := prefix
				pool.Add(func() {
					if e := src.System.Delete(src.Bucket, p); e != nil {
						common.Exit()
					}
				})
			}
			return
		}
		if src.FileType() == system.FileType_Object {
			if err := src.System.Delete(src.Bucket, src.Prefix); err != nil {
				common.Exit()
			}
		}
	},
}

// isSamePath reports whether two parsed paths name the same thing.
//
// The system pointers are compared rather than the schemes: two FileObjects
// for one scheme share the single registered backend, so this is both cheaper
// and exact. A nil system means the path did not parse, and two unparseable
// paths are not "the same object" in any useful sense.
//
// This compares the paths as given, which is what gsutil does. A backend where
// one object has more than one spelling catches its own variants -- oci does,
// for bucket versus bucket@namespace -- because only it can resolve them.
func isSamePath(a, b *system.FileObject) bool {
	if a == nil || b == nil || a.System == nil || b.System == nil {
		return false
	}
	if a.System != b.System || a.Bucket != b.Bucket {
		return false
	}
	// Trailing slashes carry no meaning here, and the difference is one
	// keystroke. gsutil treats "d" and "d/" as different because its copy
	// nests the source directory inside the destination -- it ends up with
	// d/d/... . gsg's cp -r copies the contents rather than the directory, so
	// for gsg the two name the same place: `mv -r d d/` wrote every object
	// over itself and then deleted the originals, taking two objects to none.
	return strings.TrimRight(a.Prefix, "/") == strings.TrimRight(b.Prefix, "/")
}
