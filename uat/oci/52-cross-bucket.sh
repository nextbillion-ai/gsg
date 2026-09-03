# Proves the copy path works between two different buckets.
#
# The region change reworked how a destination is addressed: Copy used to parse
# the destination's bucket and borrow the source's namespace and region, and it
# now resolves the destination in full and sends its own. Every case in
# 50-copy.sh has one bucket on both sides, which cannot tell those two apart.
#
# It also covers the half of cmd/mv.go's guard that unit tests alone cannot
# settle honestly: that resolving bucket spellings did not make it over-broad.
# A recursive move whose prefixes collide but whose buckets genuinely differ
# must still be allowed, and only two real buckets can show that.
#
# The one thing two buckets in one region cannot reach is DestinationRegion:
# with src.region == dst.region the pre-fix code and this one build identical
# requests. That is covered by TestCopyRequestNamesTheDestinationsRegionNotTheSources
# in oci/copy_test.go instead, which fails against the pre-fix addressing.
#
# Case files are sourced from inside $testbase with $mode, $remote_base,
# $testid, $oci_ns, $oci_bucket, $oci_bucket_dst and $oci_region already set.

if [[ -z "$oci_bucket_dst" ]] || ! oci os bucket get --region "$oci_region" \
    --namespace "$oci_ns" --name "$oci_bucket_dst" >/dev/null 2>&1
then
    echo "SKIP: cross-bucket cases need a second bucket in $oci_region; set GSG_UAT_OCI_BUCKET_DST"
else

# Tells the suite's cleanup that this bucket now holds test data, so that a
# failure to remove it is reported rather than shrugged off. Without this the
# cleanup cannot tell "nothing was written" from "the delete failed".
oci_dst_used=true

remote_dst="oci://$oci_bucket_dst@$oci_region/$testid"

# The second bucket is swept by the suite's own cleanup, which reports a
# failure to do so. No EXIT trap is installed here: do_test_oci owns that trap
# and it is what reports where data was left when a run fails.

start "cross-bucket: copy between two buckets"

prepare_file xb.txt "copied across buckets"
assertOk "upload to the first bucket" ../gsg cp xb.txt "$remote_base/xb/a.txt"

assertOk "copy to the second bucket" \
    ../gsg cp "$remote_base/xb/a.txt" "$remote_dst/xb/a.txt"

assertEq "the copy arrived in the second bucket" \
    "$(oci os object get --region "$oci_region" --namespace "$oci_ns" \
        --bucket-name "$oci_bucket_dst" --name "$testid/xb/a.txt" --file - 2>/dev/null)" \
    "copied across buckets"

assertEq "and the source is still in the first" \
    "$(oci os object get --region "$oci_region" --namespace "$oci_ns" \
        --bucket-name "$oci_bucket" --name "$testid/xb/a.txt" --file - 2>/dev/null)" \
    "copied across buckets"

# The destination spelled with its own namespace has to reach the same bucket.
#
# This shows that the spelling is accepted and lands where it should. It cannot
# show that the namespace was *resolved from the destination* rather than
# borrowed from the source: both buckets are in one tenancy, so the two
# namespaces are the same string. Separating those needs a bucket in another
# tenancy, which a shared credential cannot reach.
assertOk "the destination may name its own namespace" \
    ../gsg cp "$remote_base/xb/a.txt" "oci://$oci_bucket_dst@$oci_ns.$oci_region/$testid/xb/named.txt"
assertEq "and that copy arrived too" \
    "$(oci os object get --region "$oci_region" --namespace "$oci_ns" \
        --bucket-name "$oci_bucket_dst" --name "$testid/xb/named.txt" --file - 2>/dev/null)" \
    "copied across buckets"

finish

start "cross-bucket: an object at the same key in the other bucket is not itself"

# Same key, different bucket. Copy refuses a copy onto itself, and the check is
# made after resolution -- so a bucket comparison that ignored the bucket, or
# collapsed two buckets into one, would refuse this and cmd/mv.go would then
# delete a source whose copy never happened.
prepare_file samekey.txt "same key, other bucket"
assertOk "upload the source" ../gsg cp samekey.txt "$remote_base/xb/same.txt"
assertOk "copying to the same key in another bucket is a real copy" \
    ../gsg cp "$remote_base/xb/same.txt" "$remote_dst/xb/same.txt"
assertEq "and it arrived" \
    "$(oci os object get --region "$oci_region" --namespace "$oci_ns" \
        --bucket-name "$oci_bucket_dst" --name "$testid/xb/same.txt" --file - 2>/dev/null)" \
    "same key, other bucket"

finish

start "cross-bucket: move between two buckets"

prepare_file xbmove.txt "moved across buckets"
assertOk "stage the object to move" ../gsg cp xbmove.txt "$remote_base/xb/moving.txt"

assertOk "move to the second bucket" \
    ../gsg mv "$remote_base/xb/moving.txt" "$remote_dst/xb/moved.txt"

assertEq "the object arrived" \
    "$(oci os object get --region "$oci_region" --namespace "$oci_ns" \
        --bucket-name "$oci_bucket_dst" --name "$testid/xb/moved.txt" --file - 2>/dev/null)" \
    "moved across buckets"
assertEq "and the source is gone" \
    "$(oci os object head --region "$oci_region" --namespace "$oci_ns" \
        --bucket-name "$oci_bucket" --name "$testid/xb/moving.txt" >/dev/null 2>&1 \
        && echo present || echo absent)" "absent"

finish

start "cross-bucket: the move guard is not over-broad"

# The counterpart to 50-copy.sh's recursive self-move case. There, two
# spellings of ONE bucket had to be seen as one and the move refused. Here the
# prefixes collide in exactly the same shape, but the buckets are genuinely
# different -- so the move must be allowed. A guard that resolved spellings too
# eagerly, or that failed closed when it should not, would refuse this and
# leave the user unable to move anything into a similarly-named path.
prepare_file guard.txt "root"
oci os object put --region "$oci_region" --namespace "$oci_ns" --bucket-name "$oci_bucket" \
    --file guard.txt --name "$testid/xg/a.txt" --force >/dev/null 2>&1
prepare_file guard2.txt "nested"
oci os object put --region "$oci_region" --namespace "$oci_ns" --bucket-name "$oci_bucket" \
    --file guard2.txt --name "$testid/xg/sub/a.txt" --force >/dev/null 2>&1

assertOk "a recursive move into the same path in ANOTHER bucket is allowed" \
    ../gsg mv -r "$remote_base/xg" "oci://$oci_bucket_dst@$oci_ns.$oci_region/$testid/xg/sub"

assertEq "the contents arrived" \
    "$(oci os object get --region "$oci_region" --namespace "$oci_ns" \
        --bucket-name "$oci_bucket_dst" --name "$testid/xg/sub/a.txt" --file - 2>/dev/null)" "root"
assertEq "including what was nested under it" \
    "$(oci os object get --region "$oci_region" --namespace "$oci_ns" \
        --bucket-name "$oci_bucket_dst" --name "$testid/xg/sub/sub/a.txt" --file - 2>/dev/null)" "nested"
assertEq "and the source tree is gone, which is what a move means" \
    "$(oci os object head --region "$oci_region" --namespace "$oci_ns" \
        --bucket-name "$oci_bucket" --name "$testid/xg/a.txt" >/dev/null 2>&1 \
        && echo present || echo absent)" "absent"

finish

start "cross-bucket: a missing destination bucket fails before anything is copied"

# Copy resolves the destination now, so a destination that does not exist is an
# error before the work request is made. It used to be accepted and fail
# asynchronously, which reports the problem far from its cause -- and for a
# move, after the point of no return.
#
# What is asserted is that the command fails and the source survives, not what
# it printed. The message really is swallowed here, but not by this backend:
# cp resolves the destination's type first, and system.FileObject.FileType
# calls common.Exit() on error without logging it, which is TODO.md item 20 and
# behaves the same on gs and s3. Asserting on the message would pin a silence
# that a fix for item 20 will rightly break.
prepare_file nodst.txt "must not be lost"
assertOk "stage an object" ../gsg cp nodst.txt "$remote_base/xb/nodst.txt"

if ../gsg cp "$remote_base/xb/nodst.txt" "oci://no-such-bucket-$testid@$oci_region/k.txt" >/dev/null 2>&1
then
    echo "FATAL: a copy to a missing destination bucket reported success"
    exit 1
fi
echo "OK: a copy to a missing destination bucket fails"

# And a move to one must leave the source alone: the refusal has to come before
# the delete.
../gsg mv "$remote_base/xb/nodst.txt" "oci://no-such-bucket-$testid@$oci_region/k.txt" >/dev/null 2>&1 || true
assertEq "and a move to a missing bucket does not delete the source" \
    "$(oci os object get --region "$oci_region" --namespace "$oci_ns" \
        --bucket-name "$oci_bucket" --name "$testid/xb/nodst.txt" --file - 2>/dev/null)" \
    "must not be lost"

finish

start "cross-bucket: a recursive copy between buckets"

prepare_file tree1.txt "one"
assertOk "upload the first" ../gsg cp tree1.txt "$remote_base/xt/one.txt"
prepare_file tree2.txt "two"
assertOk "upload the second" ../gsg cp tree2.txt "$remote_base/xt/deeper/two.txt"

assertOk "copy the tree across" ../gsg cp -r "$remote_base/xt" "$remote_dst/xt"

assertEq "the first object arrived" \
    "$(oci os object get --region "$oci_region" --namespace "$oci_ns" \
        --bucket-name "$oci_bucket_dst" --name "$testid/xt/one.txt" --file - 2>/dev/null)" "one"
assertEq "and so did the nested one, at its own depth" \
    "$(oci os object get --region "$oci_region" --namespace "$oci_ns" \
        --bucket-name "$oci_bucket_dst" --name "$testid/xt/deeper/two.txt" --file - 2>/dev/null)" "two"

finish

fi
