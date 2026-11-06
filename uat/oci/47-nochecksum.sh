# An object with no CRC32C is not downloaded again on every rsync.
#
# OCI stores a CRC32C only when the uploader asks, and the oci CLI does not, so
# there is nothing to compare with a local file. rsync used to read that as a
# difference and download such an object in full on every run. A download now
# compares it by size and modification time, which it stamps on the local file.
#
# Case files are sourced from inside $testbase with $mode, $remote_base,
# $testid, $oci_ns, $oci_bucket and $oci_region already set.

start "no checksum: rsync does not download an unchanged object again"

fnc="folder_nochecksum"
mkdir -p $fnc
printf 'AAAA' > $fnc/a
printf 'BBBB' > $fnc/b
dd if=/dev/urandom of=$fnc/big bs=1m count=20 2>/dev/null
: > $fnc/empty

# The oci CLI on purpose: uploading with gsg would store a CRC32C.
put_cli() {
    oci os object put --region "$oci_region" --namespace "$oci_ns" --bucket-name "$oci_bucket" \
        --file "$1" --name "$testid/$fnc/src/$2" --force "${@:3}" >/dev/null 2>&1
}
put_cli $fnc/a a.txt --no-multipart
put_cli $fnc/big big.bin --part-size 8
put_cli $fnc/empty empty.txt --no-multipart
for k in a.txt big.bin empty.txt
do
    assertEq "$k really stores no CRC32C" \
        "$(oci os object head --region "$oci_region" --namespace "$oci_ns" --bucket-name "$oci_bucket" \
            --name "$testid/$fnc/src/$k" 2>/dev/null | grep -c 'opc-content-crc32c')" "0"
done

rm -rf ${fnc}_sync && mkdir -p ${fnc}_sync
../gsg -m rsync -r -v -d "$remote_base/$fnc/src" ${fnc}_sync >/dev/null 2>&1
assertEq "the first rsync -v downloads it" "$(cat ${fnc}_sync/a.txt)" "AAAA"
cmp $fnc/big ${fnc}_sync/big.bin
assertEq "including a multipart object" "$?" "0"
assertEq "and an empty one" "$(wc -c < ${fnc}_sync/empty.txt | tr -d ' ')" "0"

out=$(../gsg -m rsync -r -v -d "$remote_base/$fnc/src" ${fnc}_sync 2>&1)
assertEq "a second rsync -v finds no diff" "$(echo "$out" | grep -c 'No diff detected')" "1"
assertEq "and says the content of all three was not checked" \
    "$(echo "$out" | grep -c '3 object(s) skipped on size and modification time')" "1"
out=$(../gsg -m rsync -r -d "$remote_base/$fnc/src" ${fnc}_sync 2>&1)
assertEq "as does a plain rsync" "$(echo "$out" | grep -c 'No diff detected')" "1"

# The same size, so only the time can show the object changed. The time has
# whole seconds here, so wait for it to move.
sleep 2
put_cli $fnc/b a.txt --no-multipart
../gsg -m rsync -r -v -d "$remote_base/$fnc/src" ${fnc}_sync >/dev/null 2>&1
assertEq "rsync -v picks up a replacement of the same size" "$(cat ${fnc}_sync/a.txt)" "BBBB"

# A local change of the same size has a new time too.
sleep 2
printf 'CCCC' > ${fnc}_sync/a.txt
../gsg -m rsync -r -v -d "$remote_base/$fnc/src" ${fnc}_sync >/dev/null 2>&1
assertEq "rsync -v repairs a local file changed since" "$(cat ${fnc}_sync/a.txt)" "BBBB"

# An object with a CRC32C keeps the content check: gsg wrote this one.
../gsg cp $fnc/a "$remote_base/$fnc/crc/a.txt" >/dev/null 2>&1
rm -rf ${fnc}_crc && mkdir -p ${fnc}_crc
../gsg -m rsync -r -v -d "$remote_base/$fnc/crc" ${fnc}_crc >/dev/null 2>&1
out=$(../gsg -m rsync -r -v -d "$remote_base/$fnc/crc" ${fnc}_crc 2>&1)
assertEq "an object with a CRC32C finds no diff" "$(echo "$out" | grep -c 'No diff detected')" "1"
assertEq "and is not reported as unchecked" "$(echo "$out" | grep -c 'skipped on size')" "0"

rm -rf $fnc ${fnc}_sync ${fnc}_crc
finish
