# Copy, Move and Delete. Ground truth is the oci cli.

# oci_count <prefix> -> how many objects are under it.
#
# `--query 'length(data)'` returns an empty string rather than 0 when nothing
# matches, because the cli omits `data` entirely. Comparing that against "0"
# fails in a way that reads like the objects are still there.
oci_count() {
    local n
    n=$(oci os object list --namespace "$oci_ns" --bucket-name "$oci_bucket" \
        --prefix "$1" --all --query 'length(data)' 2>/dev/null)
    echo "${n:-0}"
}

start "copy: a cloud-to-cloud copy has finished when the command returns"

fcp="folder_copy"
mkdir -p $fcp/sub
prepare_file $fcp/a.txt "copy payload"
prepare_file $fcp/sub/b.txt "nested copy payload"
../gsg -m cp -r $fcp "$remote_base/src" >/dev/null 2>&1

# The point of this case. OCI's CopyObject is asynchronous: it returns a work
# request in about 90ms and the object appears some seconds later -- measured
# at 4.4s for a small object. Everything below checks the destination the
# instant the command returns, with no sleep, so a backend that reported
# success on acceptance would fail here.
../gsg -m cp -r "$remote_base/src" "$remote_base/dst" >/dev/null 2>&1
assertEq "both objects exist the moment cp returns" \
    "$(oci_count "$testid/dst/")" "2"
assertEq "and the copy has the source's content" \
    "$(oci os object get --namespace "$oci_ns" --bucket-name "$oci_bucket" \
        --name "$testid/dst/a.txt" --file - 2>/dev/null)" "copy payload"
assertEq "including the nested one" \
    "$(oci os object get --namespace "$oci_ns" --bucket-name "$oci_bucket" \
        --name "$testid/dst/sub/b.txt" --file - 2>/dev/null)" "nested copy payload"

finish

start "copy: a missing source is an error, not a silent no-op"

if ../gsg cp "$remote_base/src/absent.txt" "$remote_base/dst/absent.txt" >/dev/null 2>&1
then
    echo "FATAL: copying a missing object reported success"
    exit 1
else
    echo "OK: copying a missing object fails"
fi
assertEq "and wrote nothing at the destination" \
    "$(oci os object head --namespace "$oci_ns" --bucket-name "$oci_bucket" \
        --name "$testid/dst/absent.txt" >/dev/null 2>&1 && echo present || echo absent)" "absent"

finish

start "move: the source goes only after the copy is real"

# Move is Copy then Delete. If Copy returned on acceptance rather than on
# completion, the source would be deleted while the destination did not yet
# exist -- the data would simply be gone. Both halves are checked at once.
../gsg mv "$remote_base/dst/a.txt" "$remote_base/moved/a.txt" >/dev/null 2>&1
assertEq "the destination exists" \
    "$(oci os object head --namespace "$oci_ns" --bucket-name "$oci_bucket" \
        --name "$testid/moved/a.txt" >/dev/null 2>&1 && echo present || echo absent)" "present"
assertEq "with the right content" \
    "$(oci os object get --namespace "$oci_ns" --bucket-name "$oci_bucket" \
        --name "$testid/moved/a.txt" --file - 2>/dev/null)" "copy payload"
assertEq "and the source is gone" \
    "$(oci os object head --namespace "$oci_ns" --bucket-name "$oci_bucket" \
        --name "$testid/dst/a.txt" >/dev/null 2>&1 && echo present || echo absent)" "absent"

finish

start "move: moving an object onto itself must not delete it"

# gsg mv does not call System.Move: cmd/mv.go copies and then deletes the
# source itself. So a Copy that quietly did nothing for a self-copy would be
# followed by an unconditional delete -- one object in, nothing out. Measured
# before the fix, the object was simply gone.
#
# Both spellings are covered, because one object has two of them: "bucket" and
# "bucket@namespace" resolve to the same object, and comparing the raw strings
# misses it. That second spelling is the one that actually lost data.
prepare_file self.txt "must survive a self-move"
oci os object put --namespace "$oci_ns" --bucket-name "$oci_bucket" \
    --file self.txt --name "$testid/self/k.txt" --force >/dev/null 2>&1

../gsg mv "$remote_base/self/k.txt" "$remote_base/self/k.txt" >/dev/null 2>&1 || true
assertEq "an identical path leaves the object alone" \
    "$(oci os object head --namespace "$oci_ns" --bucket-name "$oci_bucket" \
        --name "$testid/self/k.txt" >/dev/null 2>&1 && echo present || echo absent)" "present"

../gsg mv "$remote_base/self/k.txt" "oci://$oci_bucket@$oci_ns/$testid/self/k.txt" >/dev/null 2>&1 || true
assertEq "and so does the same object spelled with its namespace" \
    "$(oci os object head --namespace "$oci_ns" --bucket-name "$oci_bucket" \
        --name "$testid/self/k.txt" >/dev/null 2>&1 && echo present || echo absent)" "present"
assertEq "with its content untouched" \
    "$(oci os object get --namespace "$oci_ns" --bucket-name "$oci_bucket" \
        --name "$testid/self/k.txt" --file - 2>/dev/null)" "must survive a self-move"

# And a real move must still move.
../gsg mv "$remote_base/self/k.txt" "$remote_base/self/elsewhere.txt" >/dev/null 2>&1
assertEq "a genuine move still removes the source" \
    "$(oci os object head --namespace "$oci_ns" --bucket-name "$oci_bucket" \
        --name "$testid/self/k.txt" >/dev/null 2>&1 && echo present || echo absent)" "absent"
assertEq "and delivers the destination" \
    "$(oci os object get --namespace "$oci_ns" --bucket-name "$oci_bucket" \
        --name "$testid/self/elsewhere.txt" --file - 2>/dev/null)" "must survive a self-move"

finish

start "delete: rm removes exactly what it was asked to"

../gsg rm "$remote_base/moved/a.txt" >/dev/null 2>&1
assertEq "the named object is gone" \
    "$(oci os object head --namespace "$oci_ns" --bucket-name "$oci_bucket" \
        --name "$testid/moved/a.txt" >/dev/null 2>&1 && echo present || echo absent)" "absent"
assertEq "and its neighbours are untouched" \
    "$(oci_count "$testid/src/")" "2"

../gsg -m rm -r "$remote_base/src" >/dev/null 2>&1
assertEq "rm -r removes the whole tree" \
    "$(oci_count "$testid/src/")" "0"
assertEq "and stops there" \
    "$(oci_count "$testid/dst/")" "1"

finish
