# Proves that one gsg process serves more than one region.
#
# This is what the region-in-the-path change is for. Before it, a single client
# was built from ~/.oci/config and every path resolved against that one region,
# so a bucket anywhere else read as missing; and Copy always sent the source's
# region as CopyObject's DestinationRegion, so a copy could not leave it. Both
# halves are exercised here: two regions addressed from one command, and an
# object copied across them.
#
# It needs a second bucket in a second region, which most runs will not have,
# so it skips rather than fails when one is not configured. Set
# GSG_UAT_OCI_BUCKET2 and GSG_UAT_OCI_REGION2 to enable it.
#
# Case files are sourced from inside $testbase with $mode, $remote_base,
# $testid, $oci_ns, $oci_bucket and $oci_region already set.

if [[ -z "$oci_bucket2" || -z "$oci_region2" ]]
then
    echo "SKIP: cross-region cases need GSG_UAT_OCI_BUCKET2 and GSG_UAT_OCI_REGION2"
elif [[ "$oci_region2" == "$oci_region" ]]
then
    echo "SKIP: GSG_UAT_OCI_REGION2 is the same region as GSG_UAT_OCI_REGION"
else

remote2="oci://$oci_bucket2@$oci_region2/$testid"

# Everything this case writes into the second bucket is removed at the end.
#
# No EXIT trap is installed for it: do_test_oci owns that trap, and it is what
# reports where the first bucket's data was left when a run fails. Replacing it
# would silence that. So a failed run leaves objects in the second bucket too,
# and the command to remove them is printed up front rather than at the end,
# where a failing assertion would never reach it.
cleanup_region2() {
    oci os object bulk-delete --region "$oci_region2" --namespace "$oci_ns" \
        --bucket-name "$oci_bucket2" --prefix "$testid" --force >/dev/null 2>&1 || true
}
echo "note: if these cases fail, remove the second region's data with:"
echo "      oci os object bulk-delete --region $oci_region2 --namespace $oci_ns \\"
echo "        --bucket-name $oci_bucket2 --prefix $testid --force"

start "cross-region: two regions in one process"

# Both buckets reachable from the same invocation. This is the plain
# multi-region claim, before any copying: one binary, one set of credentials,
# two endpoints.
prepare_file here.txt "written in $oci_region"
assertOk "upload to the first region" ../gsg cp here.txt "$remote_base/xr/here.txt"

prepare_file there.txt "written in $oci_region2"
assertOk "upload to the second region" ../gsg cp there.txt "$remote2/xr/there.txt"

assertEq "the first region's object is in the first bucket" \
    "$(oci os object head --region "$oci_region" --namespace "$oci_ns" --bucket-name "$oci_bucket" \
        --name "$testid/xr/here.txt" >/dev/null 2>&1 && echo present || echo absent)" "present"
assertEq "the second region's object is in the second bucket" \
    "$(oci os object head --region "$oci_region2" --namespace "$oci_ns" \
        --bucket-name "$oci_bucket2" --name "$testid/xr/there.txt" >/dev/null 2>&1 \
        && echo present || echo absent)" "present"

# And neither leaked into the other. A client pointed at the wrong region would
# have put both objects in one place.
assertEq "the second region's object is not in the first bucket" \
    "$(oci os object head --region "$oci_region" --namespace "$oci_ns" --bucket-name "$oci_bucket" \
        --name "$testid/xr/there.txt" >/dev/null 2>&1 && echo present || echo absent)" "absent"

finish

start "cross-region: copy from one region to the other"

# The copy itself. CopyObject is asynchronous and gsg waits for the object to
# exist, so a success here means it is really readable at the destination.
assertOk "copy across regions" \
    ../gsg cp "$remote_base/xr/here.txt" "$remote2/xr/copied.txt"

assertEq "the copy arrived in the second region" \
    "$(oci os object get --region "$oci_region2" --namespace "$oci_ns" \
        --bucket-name "$oci_bucket2" --name "$testid/xr/copied.txt" --file - 2>/dev/null)" \
    "written in $oci_region"

assertEq "and the source is still where it was" \
    "$(oci os object get --region "$oci_region" --namespace "$oci_ns" --bucket-name "$oci_bucket" \
        --name "$testid/xr/here.txt" --file - 2>/dev/null)" "written in $oci_region"

finish

start "cross-region: move from one region to the other"

# Move is copy-then-delete, so a cross-region move is the case where getting
# the destination region wrong loses data outright: the copy would land back in
# the source region -- or be refused as a self-copy -- and the delete would run
# regardless.
prepare_file moved.txt "must arrive in $oci_region2"
assertOk "stage the object to move" ../gsg cp moved.txt "$remote_base/xr/moved.txt"

assertOk "move across regions" \
    ../gsg mv "$remote_base/xr/moved.txt" "$remote2/xr/moved.txt"

assertEq "the object arrived in the second region" \
    "$(oci os object get --region "$oci_region2" --namespace "$oci_ns" \
        --bucket-name "$oci_bucket2" --name "$testid/xr/moved.txt" --file - 2>/dev/null)" \
    "must arrive in $oci_region2"

assertEq "and the source is gone" \
    "$(oci os object head --region "$oci_region" --namespace "$oci_ns" --bucket-name "$oci_bucket" \
        --name "$testid/xr/moved.txt" >/dev/null 2>&1 && echo present || echo absent)" "absent"

finish

start "cross-region: the object survives the round trip"

# Down from the second region and back to a file, to prove the bytes are the
# ones that were sent rather than an empty object of the right name.
rm -f roundtrip.txt
assertOk "download from the second region" \
    ../gsg cp "$remote2/xr/moved.txt" roundtrip.txt
assertEq "with its content intact" "$(cat roundtrip.txt)" "must arrive in $oci_region2"

finish

cleanup_region2

fi
