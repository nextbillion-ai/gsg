# Attributes, exercised through `gsg hash`, which is its only CLI consumer.
#
# Ground truth is the oci cli, never gsg: the checksum gsg prints is compared
# against the one the service reports, decoded independently here.

start "attrs: gsg hash reports the checksum the service stored"

# oci_crc32c_dec <object-name> -> the stored CRC32C as a decimal number, or ""
oci_crc32c_dec() {
    local b64 hex
    b64=$(oci os object head --region "$oci_region" --namespace "$oci_ns" --bucket-name "$oci_bucket" \
        --name "$1" --query '"opc-content-crc32c"' --raw-output 2>/dev/null)
    [[ -z "$b64" || "$b64" == "null" ]] && return 0
    hex=$(printf '%s' "$b64" | base64 -d 2>/dev/null | xxd -p)
    [[ -z "$hex" ]] && return 0
    echo $((16#$hex))
}

prepare_file withcrc.txt "attrs case payload"
oci os object put --region "$oci_region" --namespace "$oci_ns" --bucket-name "$oci_bucket" \
    --file withcrc.txt --name "$testid/withcrc.txt" --force \
    --opc-checksum-algorithm CRC32C >/dev/null 2>&1

expected=$(oci_crc32c_dec "$testid/withcrc.txt")
if [[ -z "$expected" ]]
then
    echo "FATAL: the service stored no CRC32C, so this case would prove nothing"
    exit 1
fi

got=$(../gsg hash "$remote_base/withcrc.txt" 2>/dev/null | awk '/Hash/ {print $NF}')
assertEq "gsg hash matches the checksum the service reports" "$got" "$expected"

# The same object addressed with an explicit namespace must give the same
# answer -- the two url forms are meant to be interchangeable.
got2=$(../gsg hash "oci://$oci_bucket@$oci_ns.$oci_region/$testid/withcrc.txt" 2>/dev/null | awk '/Hash/ {print $NF}')
assertEq "the bucket@namespace form resolves to the same object" "$got2" "$expected"

# An object stored without a checksum must not be reported as one whose
# checksum happens to be zero. Attrs cannot carry the difference (TODO 18), so
# the backend says so in the log; the number itself is 0.
prepare_file nocrc.txt "no checksum stored"
oci os object put --region "$oci_region" --namespace "$oci_ns" --bucket-name "$oci_bucket" \
    --file nocrc.txt --name "$testid/nocrc.txt" --force >/dev/null 2>&1
out=$(../gsg hash "$remote_base/nocrc.txt" 2>&1)
assertEq "a checksum-less object is called out, not silently reported as 0" \
    "$(echo "$out" | grep -c 'no CRC32C stored')" "1"

finish

start "attrs: a failure is not reported as absence"

# Note for later case files: uat.sh runs under `set -e`, and `x=$(cmd)` takes
# the exit status of cmd. Capturing a command that is *meant* to fail therefore
# kills the run with no message at all, which looks exactly like a hang. Append
# `|| true` to any such capture.

# The lesson from the s3 side (TODO items 3 and 14): a request that could not
# be answered must not read as "no such object", or callers delete or re-upload
# data that is really there. OCI makes this easy to get wrong -- a HEAD has no
# body, so a missing object, a missing bucket and a missing namespace all come
# back as a bare 404.

# A genuinely absent object: reported as absent, and no bucket error.
out=$(../gsg hash "$remote_base/definitely-absent.txt" 2>&1) || true
assertEq "an absent object reads as absent" \
    "$(echo "$out" | grep -c 'Invalid bucket')" "1"
assertEq "and is not confused with a bucket problem" \
    "$(echo "$out" | grep -c 'cannot reach bucket')" "0"

# A missing bucket: must be an error naming the bucket, not absence.
out=$(../gsg hash "oci://no-such-bucket-$testid@$oci_region/some-key" 2>&1) || true
assertEq "a missing bucket is an error, not absence" \
    "$(echo "$out" | grep -c 'cannot reach bucket')" "1"

# A wrong namespace: likewise.
out=$(../gsg hash "oci://$oci_bucket@nosuchnamespace9.$oci_region/some-key" 2>&1) || true
assertEq "a wrong namespace is an error, not absence" \
    "$(echo "$out" | grep -c 'cannot reach bucket')" "1"

finish
