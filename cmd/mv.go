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
		// A guard that cannot answer refuses. It is consulted only when the
		// destination is the source or inside it, so "cannot tell" here means
		// "cannot tell whether this is the destructive shape", and the only
		// safe reading of that is that it might be.
		destroys, derr := wouldDestroySource(src, dst)
		if derr != nil {
			logger.Info(module, "refusing to move %s to %s: the destination is the source or inside it, and whether the two name one bucket could not be established: %s",
				args[0], args[1], derr)
			common.Exit()
		}
		if destroys {
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
// The buckets are compared as the backend resolves them, not as they were
// typed. Comparing the raw strings is not enough where one bucket has more
// than one spelling: oci accepts both "b@region" and "b@namespace.region" for
// the same bucket, so `mv -r oci://b@region/d oci://b@ns.region/d/sub` looked
// like two different buckets here and sailed past the guard. Measured against
// a real bucket before the fix, with d/a.txt holding "root" and d/sub/a.txt
// holding "nested", that command left one object where there had been two.
//
// The backend's own self-copy check does not catch it either -- that compares
// one object to one object, and this is a directory to its own descendant,
// which is exactly the shape this function exists for.
//
// gs and s3 give one bucket one spelling, so they do not implement this and
// are never asked.
type bucketCanonicaliser interface {
	CanonicalBucket(spec string) (string, error)
}

// sameBucket reports whether two paths name one bucket.
//
// The order of the checks is what keeps this cheap and safe. Identical
// spellings are the overwhelmingly common case and settle it outright, so a
// backend is asked to resolve anything only when two spellings differ and the
// prefixes have already been found to collide -- which is rare, and is the
// only case where the answer can cost data.
//
// There it fails closed. Falling back to the raw strings, as the first version
// of this did, reproduces the original bug whenever resolution fails for a
// moment: the two spellings compare unequal, the guard waves the move through,
// and the copy and delete that follow resolve the namespace perfectly well on
// their next attempt. A move refused because gsg could not tell costs a retry;
// a move allowed because gsg could not tell costs an object.
func sameBucket(a, b *system.FileObject) (bool, error) {
	if a.Bucket == b.Bucket {
		return true, nil
	}
	c, ok := a.System.(bucketCanonicaliser)
	if !ok {
		// One bucket, one spelling: different strings are different buckets.
		return false, nil
	}
	ab, err := c.CanonicalBucket(a.Bucket)
	if err != nil {
		return false, err
	}
	bb, err := c.CanonicalBucket(b.Bucket)
	if err != nil {
		return false, err
	}
	return ab == bb, nil
}

// wouldDestroySource reports whether moving a onto b would lose data. The
// error is a third answer -- "cannot tell" -- and the caller must treat it as
// a refusal rather than as a false.
func wouldDestroySource(a, b *system.FileObject) (bool, error) {
	if a == nil || b == nil || a.System == nil || b.System == nil {
		return false, nil
	}
	// Two FileObjects for one scheme share the single registered backend, so
	// comparing the pointers is both cheaper and exact.
	if a.System != b.System {
		return false, nil
	}
	// The prefixes are settled before the buckets, because they are free and
	// they decide whether the buckets matter at all. Where the destination is
	// neither the source nor inside it, no pair of buckets can make this move
	// destructive -- so an ordinary move never resolves anything, and the
	// fail-closed path above can never refuse one.
	src := strings.TrimRight(a.Prefix, "/")
	dst := strings.TrimRight(b.Prefix, "/")
	if src != dst && !(src != "" && strings.HasPrefix(dst, src+"/")) {
		return false, nil
	}
	return sameBucket(a, b)
}
