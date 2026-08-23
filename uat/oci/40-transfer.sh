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

rm -rf ${fx}_down && mkdir ${fx}_down
../gsg -m cp -r "$remote_base/$fx" ${fx}_down >/dev/null 2>&1
assertOk "the downloaded tree matches the original" diff -r $fx ${fx}_down

assertEq "no temporary files were left behind" \
    "$(find ${fx}_down -name '*_.gstmp' | wc -l | tr -d ' ')" "0"

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
