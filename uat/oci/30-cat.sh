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

finish
