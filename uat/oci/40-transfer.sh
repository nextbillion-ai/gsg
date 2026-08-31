# Upload and Download. Ground truth is the oci cli and the local bytes.

start "transfer: a tree round-trips byte for byte"

fx="folder_xfer"
mkdir -p $fx/sub
prepare_file $fx/a.txt "upload payload"
prepare_file $fx/sub/b.txt "nested payload"
# Binary and a no-trailing-newline file: the two shapes a text-only case misses.
head -c 100000 /dev/urandom > $fx/blob.bin
printf 'no trailing newline' > $fx/nonl.txt

../gsg -m cp -r $fx "$remote_base/$fx" >/dev/null 2>&1
assertEq "every file was uploaded" \
    "$(oci os object list --namespace "$oci_ns" --bucket-name "$oci_bucket" \
        --prefix "$testid/$fx/" --all --query 'length(data)' 2>/dev/null)" "4"

# gsg must record a CRC32C on upload. Without one the service keeps only an
# MD5, nothing gsg compares would have a counterpart, rsync would copy the
# object again on every run and -v would verify nothing. Same fix as #47.
# Query the header rather than grepping the whole response: the string
# "opc-content-crc32c" also appears inside access-control-expose-headers, so a
# grep matches whether or not a checksum was stored.
stored_crc=$(oci os object head --namespace "$oci_ns" --bucket-name "$oci_bucket" \
    --name "$testid/$fx/a.txt" --query '"opc-content-crc32c"' --raw-output 2>/dev/null)
if [[ -n "$stored_crc" && "$stored_crc" != "null" ]]
then
    echo "OK: the upload stored a CRC32C"
else
    echo "FATAL: the upload stored no CRC32C, so nothing gsg compares has a counterpart"
    exit 1
fi

# gsg also sends that checksum, so the service checks the body on arrival
# rather than merely recording a checksum of whatever reached it. The stored
# value must therefore equal what the bytes on disk hash to -- if gsg sent a
# wrong one the upload would have been rejected outright with
# "does not match the expected CRC32C checksum", which is what makes this
# assertion meaningful rather than tautological.
local_b64=$(../gsg hash "$remote_base/$fx/a.txt" 2>/dev/null | awk '/Hash/ {print $NF}')
stored_dec=$((16#$(printf '%s' "$stored_crc" | base64 -d 2>/dev/null | xxd -p)))
assertEq "the stored checksum is the one the bytes hash to" "$local_b64" "$stored_dec"

rm -rf ${fx}_down && mkdir ${fx}_down
../gsg -m cp -r "$remote_base/$fx" ${fx}_down >/dev/null 2>&1
assertOk "the downloaded tree matches the original" diff -r $fx ${fx}_down

assertEq "no temporary files were left behind" \
    "$(find ${fx}_down -name '*_.gstmp' | wc -l | tr -d ' ')" "0"

finish

start "transfer: a changed file uploads on the first attempt"

# The checksum sent with an upload is taken from the handle the body is read
# from, not from the crc32c cache keyed on path and mtime. The cache is only as
# good as the assumption that content and modification time move together, and
# the service rejects an object whose checksum does not describe its body -- so
# a stale entry costs a whole upload spent to be told so.
#
# Its own prefix, so the counts the later cases make are unaffected.
fre="folder_reupload"
mkdir -p $fre

# Ordinary edit first: content and mtime both move.
prepare_file $fre/edited.txt first
../gsg cp $fre/edited.txt "$remote_base/$fre/edited.txt"
prepare_file $fre/edited.txt second
editout=$(../gsg cp $fre/edited.txt "$remote_base/$fre/edited.txt" 2>&1) && editrc=0 || editrc=$?
assertEq "re-uploading a changed file succeeds" "$editrc" "0"
assertEq "and was not refused and retried" \
    "$(echo "$editout" | grep -c 'does not match the expected')" "0"
assertEq "the stored object is the new content" \
    "$(oci os object get --namespace "$oci_ns" --bucket-name "$oci_bucket" \
        --name "$testid/$fre/edited.txt" --file - 2>/dev/null)" "second"

# Then the case the cache cannot see: the modification time put back exactly.
# A helper rather than touch, because touch works to the second while the cache
# key carries nanoseconds -- a touched file lands on a different key and would
# be recomputed whatever the code did.
prepare_file $fre/sneaky.txt before
../gsg cp $fre/sneaky.txt "$remote_base/$fre/sneaky.txt"
cat > sneaky_oci.go <<'GOHELPER'
package main

import (
	"os"
	"time"
)

func main() {
	p, content := os.Args[1], os.Args[2]
	fi, err := os.Stat(p)
	if err != nil {
		os.Exit(1)
	}
	mt := fi.ModTime()
	if err = os.WriteFile(p, []byte(content+"\n"), 0600); err != nil {
		os.Exit(1)
	}
	if err = os.Chtimes(p, time.Now(), mt); err != nil {
		os.Exit(1)
	}
}
GOHELPER
(cd .. && go run "$OLDPWD/sneaky_oci.go" "$OLDPWD/$fre/sneaky.txt" afterx)
rm -f sneaky_oci.go

sneakout=$(../gsg cp $fre/sneaky.txt "$remote_base/$fre/sneaky.txt" 2>&1) && sneakrc=0 || sneakrc=$?
assertEq "a change that keeps the modification time still uploads" "$sneakrc" "0"
assertEq "and is not refused either" \
    "$(echo "$sneakout" | grep -c 'does not match the expected')" "0"
assertEq "and stores the new content" \
    "$(oci os object get --namespace "$oci_ns" --bucket-name "$oci_bucket" \
        --name "$testid/$fre/sneaky.txt" --file - 2>/dev/null)" "afterx"

# Everything above would also pass if the checksum were never sent: OCI would
# compute one over whatever arrived, store it, and agree with itself forever.
# What makes the header load-bearing is that a wrong one is refused. Built with
# an overlay so the source tree is untouched -- a gsg whose checksum is one off
# from its body must have the object rejected, and must leave nothing behind.
ovdir=$(mktemp -d)
sed 's/return h.Sum32(), n, nil/return h.Sum32() + 1, n, nil/' ../oci/transfer.go > "$ovdir/transfer.go"
assertEq "the overlay actually changed the checksum" \
    "$(diff ../oci/transfer.go "$ovdir/transfer.go" | grep -c '^>')" "1"
printf '{"Replace":{"%s/oci/transfer.go":"%s/transfer.go"}}' "$repoRoot" "$ovdir" > "$ovdir/overlay.json"
(cd .. && go build -overlay "$ovdir/overlay.json" -o "$ovdir/gsg_wrongcrc" .) >/dev/null 2>&1

if "$ovdir/gsg_wrongcrc" cp $fre/edited.txt "$remote_base/$fre/wrongcrc.txt" >/dev/null 2>&1
then
    echo "FATAL: an upload carrying a checksum that does not match its body was accepted"
    rm -rf "$ovdir"
    exit 1
else
    echo "OK: an upload whose checksum does not match its body is refused"
fi
assert_not $fre/wrongcrc.txt remote
rm -rf "$ovdir"

finish

start "transfer: -v verifies, and a repeated rsync is a no-op"

assertEq "cp -v checks every downloaded file" \
    "$(../gsg -m cp -r -v "$remote_base/$fx" ${fx}_v 2>&1 | grep -c 'CRC32C checking success')" "4"

rm -rf ${fx}_sync && mkdir ${fx}_sync
../gsg -m rsync -r "$remote_base/$fx" ${fx}_sync >/dev/null 2>&1
assertEq "a second rsync copies nothing" \
    "$(../gsg -m rsync -r "$remote_base/$fx" ${fx}_sync 2>&1 | grep -c 'No diff detected')" "1"
assertEq "and a third is still a no-op" \
    "$(../gsg -m rsync -r "$remote_base/$fx" ${fx}_sync 2>&1 | grep -c 'No diff detected')" "1"
assertOk "the synced tree matches" diff -r $fx ${fx}_sync

finish

start "transfer: rsync still notices a real difference"

# An incremental sync that has stopped copying anything is worse than one that
# copies too much, so both directions are pinned.
printf 'TAMPERED' | dd of=${fx}_sync/blob.bin bs=1 seek=100 conv=notrunc 2>/dev/null
assertEq "a tampered file is copied again" \
    "$(../gsg -m rsync -r "$remote_base/$fx" ${fx}_sync 2>&1 | grep -c 'Downloading \[.*blob.bin\]: Done')" "1"
assertOk "and its contents are restored" cmp -s $fx/blob.bin ${fx}_sync/blob.bin

rm ${fx}_sync/a.txt
assertEq "a deleted file is fetched again" \
    "$(../gsg -m rsync -r "$remote_base/$fx" ${fx}_sync 2>&1 | grep -c 'Downloading \[.*a.txt\]: Done')" "1"
assertEq "and the tree is quiet once more" \
    "$(../gsg -m rsync -r "$remote_base/$fx" ${fx}_sync 2>&1 | grep -c 'No diff detected')" "1"

finish

start "transfer: mtime survives the round trip"

# The two are compared at second precision on purpose: a listing carries
# milliseconds, the last-modified header carries seconds, and mixing the two
# made every object look modified.
# TZ=UTC matters: `date -j -f` ignores the literal GMT in the format and reads
# the timestamp as local time, which put this eight hours out in Singapore.
remote_epoch=$(TZ=UTC date -j -f '%a, %d %b %Y %H:%M:%S GMT' \
    "$(oci os object head --namespace "$oci_ns" --bucket-name "$oci_bucket" \
        --name "$testid/$fx/a.txt" --query '"last-modified"' --raw-output 2>/dev/null)" \
    +%s 2>/dev/null)
local_epoch=$(stat -f %m ${fx}_down/a.txt)
assertEq "the local file carries the object's mtime" "$local_epoch" "$remote_epoch"

finish
