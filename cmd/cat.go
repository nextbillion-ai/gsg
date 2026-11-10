package cmd

import (
	"fmt"
	"os"
	"strings"

	"github.com/nextbillion-ai/gsg/common"
	"github.com/nextbillion-ai/gsg/system"

	"github.com/spf13/cobra"
)

var catWindow int

func init() {
	catCmd.Flags().IntVar(&catWindow, "window", common.DefaultStreamWindow,
		"chunks fetched ahead of the one being written; memory is window x chunk size")
	rootCmd.AddCommand(catCmd)
}

var catCmd = &cobra.Command{
	Use:   "cat [url]",
	Short: "Write an object to stdout as it arrives",
	Long: fmt.Sprintf(`Write an object to stdout as it arrives.

The object is read in ranged chunks, up to --window of them in parallel, and
written in order as each arrives, so the first byte goes out with the first
response and memory stays at window x chunk size (%d x %d MiB by default).
--chunk-size 0 reads it as a single stream. A chunk whose read fails is
reopened where it stopped and retried for about 15 s.

Every range reads the version the first lookup saw (the generation on gs, the
ETag on s3 and oci), so an overwrite during the read fails it rather than
mixing two versions.

The output is checked against the object's CRC32C (on oci without one, the MD5
of a single-part upload; on s3 without a whole-object CRC32C, the length only).
The bytes are written before the check can finish, so a mismatch is reported
after the last byte and the command exits non-zero: treat the output as the
object only if gsg exits 0. Log lines go to stderr.`,
		common.DefaultStreamWindow, common.DefaultStreamChunkSize>>20),
	Args: cobra.ExactArgs(1),
	Run: func(_ *cobra.Command, args []string) {
		fo := system.ParseFileObject(args[0])
		if fo == nil {
			common.Exit()
			return
		}
		err := fo.System.Cat(fo.Bucket, fo.Prefix, os.Stdout, common.StreamOptions{
			ChunkSize: chunkSize,
			Window:    catWindow,
		})
		if err != nil {
			common.ExitWith(err)
		}
	},
}

// isCatCommand reports whether the subcommand in args is cat, so that logging
// can leave stdout before anything is logged.
func isCatCommand(args []string) bool {
	takesValue := map[string]bool{"-c": true, "--c": true, "--chunk-size": true, "--window": true}
	for i := 1; i < len(args); i++ {
		a := args[i]
		if strings.HasPrefix(a, "-") {
			if takesValue[a] {
				i++
			}
			continue
		}
		return a == "cat"
	}
	return false
}
