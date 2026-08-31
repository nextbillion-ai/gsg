# Cat, IsObject and IsDirectory. Ground truth is the oci cli.

start "cat: bytes come back exactly"

fcat="folder_cat"
mkdir -p $fcat
prepare_file $fcat/text.txt "plain text payload"
# Binary matters on its own: NUL bytes and a missing trailing newline are what
# a naive read-into-a-string mangles, and a text-only case would not notice.
head -c 4096 /dev/urandom > $fcat/blob.bin
printf 'no trailing newline' > $fcat/nonl.txt

for f in text.txt blob.bin nonl.txt
do
    oci os object put --namespace "$oci_ns" --bucket-name "$oci_bucket" \
        --file $fcat/$f --name "$testid/$fcat/$f" --force >/dev/null 2>&1
done

for f in text.txt blob.bin nonl.txt
do
    ../gsg cat "$remote_base/$fcat/$f" > got_$f 2>/dev/null
    assertOk "cat $f returns the file byte for byte" cmp -s got_$f $fcat/$f
done

finish

start "cat: a missing object fails rather than printing nothing"

# An empty stdout and exit 0 would look exactly like an empty file, so the
# failure has to be visible in the exit code.
if ../gsg cat "$remote_base/$fcat/absent.txt" >/dev/null 2>&1
then
    echo "FATAL: cat of a missing object reported success"
    exit 1
else
    echo "OK: cat of a missing object fails"
fi

finish

start "isobject and isdirectory: a path is classified correctly"

# Reached through cp, which asks FileType before doing anything: a directory
# without -r is refused with a distinctive message.
oci os object put --namespace "$oci_ns" --bucket-name "$oci_bucket" \
    --file $fcat/text.txt --name "$testid/edge/abc.txt" --force >/dev/null 2>&1
oci os object put --namespace "$oci_ns" --bucket-name "$oci_bucket" \
    --file $fcat/text.txt --name "$testid/edge/dir/inner.txt" --force >/dev/null 2>&1

isdir() {
    local out
    out=$(../gsg cp "$remote_base/$1" ./sink 2>&1) || true
    echo "$out" | grep -qc "Did you mean" >/dev/null && echo yes || echo no
}

assertEq "a prefix with objects under it is a directory" "$(isdir edge)" "yes"
assertEq "the same prefix with a trailing slash too" "$(isdir edge/)" "yes"
assertEq "a nested directory is a directory" "$(isdir edge/dir)" "yes"

# The subtle ones: a partial name must not match. Without the trailing slash a
# plain prefix match would make "edge/ab" look like a directory containing
# "edge/abc.txt", and "edge/d" like one containing "edge/dir/".
assertEq "a partial object name is not a directory" "$(isdir edge/ab)" "no"
assertEq "a partial directory name is not a directory" "$(isdir edge/d)" "no"
assertEq "an object is not a directory" "$(isdir edge/abc.txt)" "no"
assertEq "something absent is not a directory" "$(isdir edge/nothing-here)" "no"

# An object whose name is exactly the prefix -- the zero-byte marker a "create
# folder" writes -- is the prefix, not something beneath it. gsutil hands it
# back as an object, and gs and s3 agree, so oci does too. It sorts before
# everything under it, which is the trap: asking for a single entry returns
# only the marker, so a directory carrying one would look empty. Measured
# against this fixture: listing "both/" at limit 1 returns just the marker, at
# limit 2 the marker and its child.
: > empty_marker
oci os object put --namespace "$oci_ns" --bucket-name "$oci_bucket" \
    --file empty_marker --name "$testid/marker/lone/" --force >/dev/null 2>&1
oci os object put --namespace "$oci_ns" --bucket-name "$oci_bucket" \
    --file empty_marker --name "$testid/marker/both/" --force >/dev/null 2>&1
rm -f empty_marker
oci os object put --namespace "$oci_ns" --bucket-name "$oci_bucket" \
    --file $fcat/text.txt --name "$testid/marker/both/child.txt" --force >/dev/null 2>&1

assertEq "a marker with nothing beneath it is an object, not a directory" \
    "$(isdir marker/lone/)" "no"
assertEq "a marker with something beneath it is still a directory" \
    "$(isdir marker/both/)" "yes"
assertEq "the same marker named without a trailing slash is a directory" \
    "$(isdir marker/lone)" "yes"

# A directory whose children are all sub-directories carries no objects in a
# delimited listing, only prefixes -- so the Prefixes arm has to be read, and
# nothing above would notice if it were dropped.
oci os object put --namespace "$oci_ns" --bucket-name "$oci_bucket" \
    --file $fcat/text.txt --name "$testid/subsonly/only/deeper/x.txt" --force >/dev/null 2>&1
assertEq "a directory holding only sub-directories is a directory" \
    "$(isdir subsonly)" "yes"

finish

start "isdirectory: a directory bigger than one page is still just one question"

# IsDirectory asks whether anything exists under a path, so it asks for a
# single entry. Answering it by listing and counting costs a page per thousand
# objects -- measured at 237ms for 1005 objects, and minutes for a prefix
# holding a million. FileType calls this before nearly every command, so the
# cost lands on cp, rm, du, mv and rsync alike.
#
# This case pins the correctness half at a size past one page. The cost half
# cannot be asserted from a shell, but a regression to listing-and-counting
# would show up here as a visibly slower run.
fbig="folder_big"
mkdir -p $fbig
i=1
while [[ $i -le 1005 ]]
do
    printf 'x' > "$fbig/f$(printf '%04d' $i).txt"
    i=$((i + 1))
done
oci os object bulk-upload --namespace "$oci_ns" --bucket-name "$oci_bucket" \
    --src-dir $fbig --object-prefix "$testid/$fbig/" --overwrite \
    --parallel-upload-count 40 >/dev/null 2>&1

assertEq "the provider really has more than one page of objects" \
    "$(oci os object list --namespace "$oci_ns" --bucket-name "$oci_bucket" \
        --prefix "$testid/$fbig/" --all --query 'length(data)' 2>/dev/null)" "1005"

assertEq "a directory of 1005 objects is a directory" "$(isdir $fbig)" "yes"
assertEq "and one of its objects is not" "$(isdir $fbig/f0001.txt)" "no"
assertEq "nor is a partial name inside it" "$(isdir $fbig/f000)" "no"

oci os object bulk-delete --namespace "$oci_ns" --bucket-name "$oci_bucket" \
    --prefix "$testid/$fbig/" --force >/dev/null 2>&1
rm -rf $fbig

finish
