# List, through `gsg ls`. Ground truth is the oci cli.

start "list: objects and immediate directories"

flist="folder_list"
mkdir -p $flist/sub/deep
prepare_file $flist/a.txt
prepare_file $flist/b.txt
prepare_file $flist/sub/c.txt
prepare_file $flist/sub/deep/d.txt
oci os object bulk-upload --namespace "$oci_ns" --bucket-name "$oci_bucket" \
    --src-dir $flist --object-prefix "$testid/$flist/" --overwrite >/dev/null 2>&1

assertEq "the provider really has 4 objects" \
    "$(oci os object list --namespace "$oci_ns" --bucket-name "$oci_bucket" \
        --prefix "$testid/$flist/" --all --query 'length(data)' 2>/dev/null)" "4"

# Without -r: the two objects directly under the prefix, plus sub/ as a
# directory. Not the four objects, and not sub/ twice.
assertEq "ls lists immediate children only" \
    "$(../gsg ls "$remote_base/$flist" 2>/dev/null | wc -l | tr -d ' ')" "3"
assertEq "and shows the sub-directory as a directory" \
    "$(../gsg ls "$remote_base/$flist" 2>/dev/null | grep -c '/sub/$')" "1"
assertEq "ls -r reaches every object" \
    "$(../gsg ls -r "$remote_base/$flist" 2>/dev/null | wc -l | tr -d ' ')" "4"
assertEq "ls -r lists no directories" \
    "$(../gsg ls -r "$remote_base/$flist" 2>/dev/null | grep -c '/$')" "0"
assertEq "nothing is listed twice" \
    "$(../gsg ls -r "$remote_base/$flist" 2>/dev/null | sort | uniq -d | wc -l | tr -d ' ')" "0"

finish

start "list: how a directory marker is reported"

# Some tools write a zero-length "folder/" object. Measured against the
# service: a delimited listing absorbs it into the common prefix, so it is not
# an extra row; an undelimited one returns it, so ls -r does show it. The s3
# backend lists it the same way, and this case pins both halves so a change in
# either is noticed.
: > empty_marker
oci os object put --namespace "$oci_ns" --bucket-name "$oci_bucket" \
    --file empty_marker --name "$testid/$flist/sub/" --force >/dev/null 2>&1
assertEq "the marker really exists" \
    "$(oci os object list --namespace "$oci_ns" --bucket-name "$oci_bucket" \
        --prefix "$testid/$flist/sub/" --all --query 'length(data)' 2>/dev/null)" "3"

assertEq "ls reports sub/ exactly once, not once per marker" \
    "$(../gsg ls "$remote_base/$flist" 2>/dev/null | grep -c '/sub/$')" "1"
assertEq "and ls still shows exactly three entries" \
    "$(../gsg ls "$remote_base/$flist" 2>/dev/null | wc -l | tr -d ' ')" "3"

# ls -r surfaces the marker, as it does on s3. Asserting the count pins the
# behaviour: 4 real objects plus the marker.
assertEq "ls -r surfaces the marker, matching s3" \
    "$(../gsg ls -r "$remote_base/$flist" 2>/dev/null | grep -c '/sub/$')" "1"
assertEq "ls -r returns every object and the marker, once each" \
    "$(../gsg ls -r "$remote_base/$flist" 2>/dev/null | wc -l | tr -d ' ')" "5"

# Listing the directory that holds the marker: the service returns the marker
# as an object because the caller asked for that directory by name.
assertEq "listing the marker's own directory works" \
    "$(../gsg ls "$remote_base/$flist/sub" 2>/dev/null | wc -l | tr -d ' ')" "3"

oci os object delete --namespace "$oci_ns" --bucket-name "$oci_bucket" \
    --name "$testid/$flist/sub/" --force >/dev/null 2>&1

finish

start "list: a listing that spans more than one page"

# ListObjects returns at most 1000 entries per page. A loop that stops when a
# page carries no objects is wrong for a delimited listing -- a page whose keys
# all collapsed into common prefixes has none -- and a prefix with more than a
# page of subdirectories then loses the rest silently. Measured on the s3 side
# before #45: 1000 of 1005.
fpage="folder_paged"
mkdir -p $fpage
i=1
while [[ $i -le 1005 ]]
do
    d=$fpage/$(printf 'd%04d' $i)
    mkdir -p "$d" && printf 'x' > "$d/f.txt"
    i=$((i + 1))
done
# Uploaded with the provider cli: it parallelises, and what matters is what
# gsg reads back, not how the objects got there.
oci os object bulk-upload --namespace "$oci_ns" --bucket-name "$oci_bucket" \
    --src-dir $fpage --object-prefix "$testid/$fpage/" --overwrite \
    --parallel-upload-count 40 >/dev/null 2>&1

assertEq "the provider really has 1005 objects" \
    "$(oci os object list --namespace "$oci_ns" --bucket-name "$oci_bucket" \
        --prefix "$testid/$fpage/" --all --query 'length(data)' 2>/dev/null)" "1005"
assertEq "ls without -r returns every subdirectory" \
    "$(../gsg ls "$remote_base/$fpage" 2>/dev/null | wc -l | tr -d ' ')" "1005"
assertEq "ls -r returns every object" \
    "$(../gsg ls -r "$remote_base/$fpage" 2>/dev/null | wc -l | tr -d ' ')" "1005"
assertEq "and none of them twice" \
    "$(../gsg ls -r "$remote_base/$fpage" 2>/dev/null | sort | uniq -d | wc -l | tr -d ' ')" "0"

oci os object bulk-delete --namespace "$oci_ns" --bucket-name "$oci_bucket" \
    --prefix "$testid/$fpage/" --force >/dev/null 2>&1
rm -rf $fpage

finish
