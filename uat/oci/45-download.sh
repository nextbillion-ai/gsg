# Download: parallel ranged chunks, pinned to one version, paced when asked.
#
# Until this landed an oci download was a single unranged GetObject and one
# io.Copy: one connection, one congestion window, no pool, and --gentle-io
# silently inert on this backend. These cases hold the new shape to the only
# thing that matters about it -- that the file assembled from many ranges is
# the file that was uploaded.
#
# Random content throughout, NOT zeroes, and that is the point. With uniform
# content a chunk written at the wrong offset, or two chunks transposed,
# produces a byte-identical file, so cmp, -v and rsync would all pass against
# thoroughly broken code.

start "download: many ranged chunks reassemble the object byte for byte"

fdl="folder_download"
mkdir -p $fdl
dd if=/dev/urandom of=$fdl/chunky.bin bs=1m count=20 2>/dev/null
../gsg cp $fdl/chunky.bin "$remote_base/$fdl/chunky.bin" >/dev/null 2>&1
assertEq "the fixture uploaded" "$(remote_size $fdl/chunky.bin)" "$((20 * 1024 * 1024))"

# 1 MiB chunks over 20 MiB: twenty ranged GETs rather than one stream.
dlout=$(../gsg --debug -m --chunk-size 1048576 cp -v "$remote_base/$fdl/chunky.bin" $fdl/chunky.down 2>&1) && dlrc=0 || dlrc=$?
assertEq "a chunked download exits cleanly" "$dlrc" "0"
assertEq "and it really was cut into twenty chunks" \
    "$(echo "$dlout" | grep -c 'with 20 chunk(s)')" "1"
assertOk "the file is byte for byte the original" cmp $fdl/chunky.bin $fdl/chunky.down
assertEq "-v verified it against the checksum the first HEAD reported" \
    "$(echo "$dlout" | grep -c 'CRC32C checking success')" "1"

# RFC 7233 range endpoints are inclusive. The s3 backend asks for
# bytes=start-(start+length), so every chunk there fetches one byte more than
# it needs -- which corrupts nothing, since the neighbour rewrites the shared
# byte with the same value, and so goes unnoticed. Here each chunk checks what
# it was given against what it asked for, so the overshoot is an error rather
# than a silent extra byte: a clean exit above is what proves the arithmetic.
assertEq "no chunk was served more than it asked for" \
    "$(echo "$dlout" | grep -c 'bytes for the')" "0"

# A size that is not a whole number of chunks: the last one is short.
dlout=$(../gsg --debug -m --chunk-size 3000000 cp "$remote_base/$fdl/chunky.bin" $fdl/chunky.odd 2>&1) && dlrc=0 || dlrc=$?
assertEq "an object that does not divide evenly downloads too" "$dlrc" "0"
assertEq "and its last chunk is the short one" \
    "$(echo "$dlout" | grep -c 'with 7 chunk(s)')" "1"
assertOk "it is also byte for byte the original" cmp $fdl/chunky.bin $fdl/chunky.odd

finish

start "download: --chunk-size 0 asks for the object in one request"

dlout=$(../gsg --debug --chunk-size 0 cp -v "$remote_base/$fdl/chunky.bin" $fdl/chunky.single 2>&1) && dlrc=0 || dlrc=$?
assertEq "an unchunked download exits cleanly" "$dlrc" "0"
assertEq "and it was one chunk" "$(echo "$dlout" | grep -c 'with 1 chunk(s)')" "1"
assertOk "the file matches" cmp $fdl/chunky.bin $fdl/chunky.single

finish

start "download: an empty object is fetched without a range"

: > $fdl/empty.bin
../gsg cp $fdl/empty.bin "$remote_base/$fdl/empty.bin" >/dev/null 2>&1
# The chunk geometry floors at one chunk, so an empty object still produces a
# single chunk of length 0. Asked for as a range that would be "bytes=0--1",
# which is not a range and the service refuses it, so this fails outright if
# the empty case is not handled before the range is built.
dlout=$(../gsg --debug --chunk-size 1048576 cp -v "$remote_base/$fdl/empty.bin" $fdl/empty.down 2>&1) && dlrc=0 || dlrc=$?
assertEq "an empty object downloads cleanly" "$dlrc" "0"
assertEq "and lands as an empty file" "$(wc -c < $fdl/empty.down | tr -d ' ')" "0"

finish

start "download: --gentle-io reaches this backend, and still verifies"

# Two things at once. Before this, GentleIO was read by gcs and s3 and by
# nothing in oci, so --gentle-io from oci:// was accepted and paced nothing --
# worse than not offering it, since the caller believes the transfer is paced.
#
# And gentle mode cannot verify the way the ordinary path does: it has spent
# the whole transfer asking the kernel to drop these pages, so reading the file
# back would come straight off the disk, as large as the file. It sums each
# window from the file while the pages are still cached instead, and folds
# those sums afterwards. A fold that got the order or the lengths wrong gives a
# checksum that is wrong in a way no cmp would notice -- so it is -v passing
# here, on a file cmp also agrees with, that says the fold is right.
gout=$(../gsg --debug -m --gentle-io --chunk-size 1048576 cp -v "$remote_base/$fdl/chunky.bin" $fdl/chunky.gentle 2>&1) && grc=0 || grc=$?
assertEq "a gentle download exits cleanly" "$grc" "0"
assertEq "and it was chunked the same way" "$(echo "$gout" | grep -c 'with 20 chunk(s)')" "1"
assertOk "the file is byte for byte the original" cmp $fdl/chunky.bin $fdl/chunky.gentle
assertEq "and -v agrees, from the sums it took while writing" \
    "$(echo "$gout" | grep -c 'CRC32C checking success')" "1"

# The empty and single-chunk shapes go through the same code: a window that
# never closes, and one partial window.
gout=$(../gsg --gentle-io cp -v "$remote_base/$fdl/empty.bin" $fdl/empty.gentle 2>&1) && grc=0 || grc=$?
assertEq "an empty object survives gentle mode" "$grc" "0"
assertEq "and lands empty" "$(wc -c < $fdl/empty.gentle | tr -d ' ')" "0"

finish

start "download: what a chunked download leaves behind is a finished file"

# The temporary file is renamed into place only once every chunk has landed,
# and the object's modification time is applied after the rename. Both are what
# make the next rsync a no-op: a wrong mtime, or a checksum cached against the
# wrong one, copies everything again forever.
assertEq "no temporary files were left behind" \
    "$(find $fdl -name '*_.gstmp' | wc -l | tr -d ' ')" "0"

rm -rf ${fdl}_sync && mkdir -p ${fdl}_sync
../gsg -m --chunk-size 1048576 rsync -r "$remote_base/$fdl" ${fdl}_sync >/dev/null 2>&1
assertOk "the synced tree matches the originals" cmp $fdl/chunky.bin ${fdl}_sync/chunky.bin
assertEq "a second rsync copies nothing" \
    "$(../gsg -m --chunk-size 1048576 rsync -r "$remote_base/$fdl" ${fdl}_sync 2>&1 | grep -c 'No diff detected')" "1"
assertEq "and a gentle rsync over the same tree is still a no-op" \
    "$(../gsg -m --gentle-io --chunk-size 1048576 rsync -r "$remote_base/$fdl" ${fdl}_sync 2>&1 | grep -c 'No diff detected')" "1"

finish

start "download: a multipart upload comes back through the chunked path"

# The two halves of a large transfer meet here: an object stored in parts, then
# fetched in ranges that have nothing to do with those part boundaries. The
# whole-object CRC32C has to survive both.
fdm="folder_download_mp"
mkdir -p $fdm
dd if=/dev/urandom of=$fdm/big.bin bs=1m count=130 2>/dev/null
../gsg -m cp $fdm/big.bin "$remote_base/$fdm/big.bin" >/dev/null 2>&1
assertEq "the fixture went up in parts" "$(is_multipart $fdm/big.bin)" "yes"

mout=$(../gsg --debug -m --chunk-size 16777216 cp -v "$remote_base/$fdm/big.bin" $fdm/big.down 2>&1) && mrc=0 || mrc=$?
assertEq "it downloads cleanly in ranges of its own" "$mrc" "0"
assertEq "in nine chunks, which are not the two parts it was stored as" \
    "$(echo "$mout" | grep -c 'with 9 chunk(s)')" "1"
assertOk "and it is byte for byte the original" cmp $fdm/big.bin $fdm/big.down
assertEq "-v verified the whole object" "$(echo "$mout" | grep -c 'CRC32C checking success')" "1"

rm -rf $fdm
finish

start "download: a library caller gets chunks, and neither a pool nor a bar"

# The shape jam-core uses: system.RunContext{ChunkSize: ...} and nothing else.
# The cli always supplies a Pool and a Bars, so nothing above reaches this, and
# both absences are hazards rather than defaults -- ctx.Pool.AddWithDepth on a
# nil pool is a nil dereference, and a nil *bar.ProgressBar handed over as an
# io.Writer is NOT a nil io.Writer, so writing to it dereferences the nil
# receiver inside IncrBy. Gentle mode as well, because that is the path that
# writes to the bar directly.
cat > dlprobe.go <<'GO'
package main

import (
	"fmt"
	"os"
	"strconv"

	"github.com/nextbillion-ai/gsg/oci"
	"github.com/nextbillion-ai/gsg/system"
)

func main() {
	bucket, prefix, dst := os.Args[1], os.Args[2], os.Args[3]
	chunk, err := strconv.ParseInt(os.Args[4], 10, 64)
	if err != nil {
		fmt.Println("FAIL args:", err)
		os.Exit(1)
	}
	o := &oci.OCI{}
	ctx := system.RunContext{ChunkSize: chunk, GentleIO: os.Args[5] == "gentle"}
	if derr := o.Download(bucket, prefix, dst, true, ctx); derr != nil {
		fmt.Println("FAIL download:", derr)
		os.Exit(1)
	}
	fmt.Println("PASS")
}
GO

libbucket="$oci_bucket@$oci_region"
out=$(cd .. && go run "$OLDPWD/dlprobe.go" "$libbucket" "$testid/$fdl/chunky.bin" "$OLDPWD/$fdl/chunky.lib" 1048576 plain 2>&1 | tail -1) || true
assertEq "a library download with no pool and no bar succeeds" "$out" "PASS"
assertOk "and lands the right bytes" cmp $fdl/chunky.bin $fdl/chunky.lib

out=$(cd .. && go run "$OLDPWD/dlprobe.go" "$libbucket" "$testid/$fdl/chunky.bin" "$OLDPWD/$fdl/chunky.libgentle" 1048576 gentle 2>&1 | tail -1) || true
assertEq "so does a gentle one" "$out" "PASS"
assertOk "and it lands the right bytes too" cmp $fdl/chunky.bin $fdl/chunky.libgentle

rm -f dlprobe.go
finish

rm -rf $fdl ${fdl}_sync
