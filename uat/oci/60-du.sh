# DiskUsage, through `gsg du`. Ground truth is the oci cli's own size sum.

start "du: totals match the service, at every level of the tree"

fdu="folder_du"
mkdir -p $fdu/sub/deep
# Distinct, exact sizes so a wrong total names itself: any subset sums to a
# different number.
printf '%0.sA' $(seq 1 100)  > $fdu/a.txt
printf '%0.sB' $(seq 1 250)  > $fdu/sub/b.txt
printf '%0.sC' $(seq 1 1000) > $fdu/sub/deep/c.txt

../gsg -m cp -r $fdu "$remote_base/$fdu" >/dev/null 2>&1

expected=$(oci os object list --namespace "$oci_ns" --bucket-name "$oci_bucket" \
    --prefix "$testid/$fdu/" --all --query 'sum(data[].size)' 2>/dev/null)
assertEq "the provider agrees on the total" "$expected" "1350"

assertEq "du -s reports the whole tree" \
    "$(../gsg du -s "$remote_base/$fdu" 2>/dev/null | awk '{print $1}')" "$expected"

# Intermediate directories have to be totalled too, not just leaves: a tree
# that reports the right grand total can still have the levels wrong.
du_of() { ../gsg du "$remote_base/$fdu" 2>/dev/null | awk -v p="$1" '$2 ~ p"$" {print $1}'; }
assertEq "a leaf object reports its own size" "$(du_of '/a.txt')" "100"
assertEq "a subdirectory totals what is under it" "$(du_of '/sub/')" "1250"
assertEq "the deepest directory too" "$(du_of '/sub/deep/')" "1000"

assertEq "every directory and object is reported exactly once" \
    "$(../gsg du "$remote_base/$fdu" 2>/dev/null | sort | uniq -d | wc -l | tr -d ' ')" "0"

finish

start "du: a path naming an object is its own answer"

# Checked before listing: a prefix search for "a.txt" would also match
# "a.txt.bak", so an object's du must not depend on what else is named like it.
oci os object put --namespace "$oci_ns" --bucket-name "$oci_bucket" \
    --file $fdu/sub/b.txt --name "$testid/$fdu/a.txt.bak" --force >/dev/null 2>&1
assertEq "du of an object reports only that object" \
    "$(../gsg du "$remote_base/$fdu/a.txt" 2>/dev/null | awk '{print $1}')" "100"
oci os object delete --namespace "$oci_ns" --bucket-name "$oci_bucket" \
    --name "$testid/$fdu/a.txt.bak" --force >/dev/null 2>&1

finish

start "du: a total spanning more than one page is complete"

# The same shape as the listing case: a short total looks exactly like a
# correct one, so it has to be checked against a tree bigger than a page.
fdup="folder_du_paged"
mkdir -p $fdup
i=1
while [[ $i -le 1005 ]]
do
    printf 'x' > "$fdup/$(printf 'f%04d' $i).txt"
    i=$((i + 1))
done
oci os object bulk-upload --namespace "$oci_ns" --bucket-name "$oci_bucket" \
    --src-dir $fdup --object-prefix "$testid/$fdup/" --overwrite \
    --parallel-upload-count 40 >/dev/null 2>&1

assertEq "the provider has 1005 one-byte objects" \
    "$(oci os object list --namespace "$oci_ns" --bucket-name "$oci_bucket" \
        --prefix "$testid/$fdup/" --all --query 'sum(data[].size)' 2>/dev/null)" "1005"
assertEq "du -s counts every one of them" \
    "$(../gsg du -s "$remote_base/$fdup" 2>/dev/null | awk '{print $1}')" "1005"

oci os object bulk-delete --namespace "$oci_ns" --bucket-name "$oci_bucket" \
    --prefix "$testid/$fdup/" --force >/dev/null 2>&1
rm -rf $fdup

finish
