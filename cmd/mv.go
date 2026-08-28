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

		// Refuse a move whose destination is the source, or lives inside it.
		//
		// mv is a copy followed by a delete of the source, and gsg's cp -r
		// copies a directory's *contents* rather than the directory itself.
		// That flattening is what makes a self-descendant move unsafe here:
		// source keys and destination keys collide. Measured, with d/a.txt
		// holding "root" and d/sub/a.txt holding "nested", `mv -r d d/sub`
		// left only d/sub/sub/a.txt and "root" was gone -- d/sub/a.txt was
		// written by the copy and then deleted as a source, and the two copies
		// race in the pool besides.
		//
		// gsutil allows the same command because its copy nests the source
		// directory, ending at d/sub/d/... where nothing collides. gsg cannot
		// borrow that answer without changing what cp -r means, so it refuses
		// instead of doing something destructive.
		//
		// gsutil does refuse the identical-path case, with "are the same file
		// - abort", and so does this.
		if wouldDestroySource(src, dst) {
			logger.Info(module, "refusing to move %s to %s: the destination is the source, or inside it, and the copy would overwrite what the delete then removes",
				args[0], args[1])
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
				common.ExitWith(lerr)
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
						common.ExitWith(e)
					}
				})
			}
			return
		}
		if src.FileType() == system.FileType_Object {
			if err := src.System.Delete(src.Bucket, src.Prefix); err != nil {
				common.ExitWith(err)
			}
		}
	},
}

// wouldDestroySource reports whether moving a onto b would lose data rather
// than move it.
//
// Two shapes qualify. The destination being the source is the obvious one: the
// copy produces nothing for the delete to spare. The destination living inside
// the source is the subtle one, and it is specific to how gsg copies -- cp -r
// writes a directory's contents into the destination rather than nesting the
// directory, so d/a.txt and d/sub/a.txt both want to become d/sub/a.txt.
//
// Trailing slashes are trimmed because they carry no meaning here and the
// difference is one keystroke. The "inside" test needs a slash boundary, so a
// sibling whose name merely begins with the source's is left alone: "a.txt" to
// "a.txt.bak" is a real move, and so is "d" to "dsub".
//
// The paths are compared as given. A backend where one object has more than
// one spelling catches its own variants -- oci does, for bucket versus
// bucket@namespace -- because only it can resolve them.
func wouldDestroySource(a, b *system.FileObject) bool {
	if a == nil || b == nil || a.System == nil || b.System == nil {
		return false
	}
	// Two FileObjects for one scheme share the single registered backend, so
	// comparing the pointers is both cheaper and exact.
	if a.System != b.System || a.Bucket != b.Bucket {
		return false
	}
	src := strings.TrimRight(a.Prefix, "/")
	dst := strings.TrimRight(b.Prefix, "/")
	if src == dst {
		return true
	}
	return src != "" && strings.HasPrefix(dst, src+"/")
}
