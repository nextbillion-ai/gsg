# Proves that an oci:// path has to say which region it means.
#
# The region decides which of possibly several buckets of one name is meant,
# and OCI offers no way to discover it -- a bucket resource carries no region
# field, because its region is whichever regional endpoint answered. So gsg
# takes it from the path or refuses the path. What is checked here is that it
# really does refuse, and refuses in a way the user can act on: these errors
# are the entire migration story for anyone with existing oci:// paths.
#
# Case files are sourced from inside $testbase with $mode, $remote_base,
# $testid, $oci_ns, $oci_bucket and $oci_region already set.

start "addressing: a path must name its region"

# The old spelling. It must not quietly resolve against ~/.oci/config, which
# is exactly what it used to do -- and what made one path mean different things
# on different machines.
out=$(../gsg hash "oci://$oci_bucket/$testid/anything.txt" 2>&1) || true
assertEq "a path with no region is refused" \
    "$(echo "$out" | grep -c 'no region')" "1"
assertEq "and the message shows the spelling that works" \
    "$(echo "$out" | grep -c "$oci_bucket@<region>")" "1"

# A short region name is refused rather than expanded. Of the 78 short codes
# the SDK knows, 18 pairs are one keystroke apart -- sin/lin, iad/mad,
# hyd/syd -- so accepting them means a slip can name a different live region,
# pass validation, and surface much later as a missing bucket.
out=$(../gsg hash "oci://$oci_bucket@sin/$testid/anything.txt" 2>&1) || true
assertEq "a short region name is refused" \
    "$(echo "$out" | grep -c 'short region name')" "1"
assertEq "and the message names the full form to use" \
    "$(echo "$out" | grep -c 'ap-singapore-1')" "1"

# Something that is not a region at all is caught where it was written, rather
# than becoming a connection error against a hostname that does not exist.
out=$(../gsg hash "oci://$oci_bucket@not-a-region/$testid/anything.txt" 2>&1) || true
assertEq "a malformed region is refused" \
    "$(echo "$out" | grep -c 'is not a region name')" "1"

# Every one of these errors has to quote the path. They surface from workers
# deep inside a recursive operation, where "not a region name" on its own
# leaves the user hunting for which of many paths was wrong.
for bad in "oci://$oci_bucket/k" "oci://$oci_bucket@sin/k" "oci://$oci_bucket@not-a-region/k"
do
    out=$(../gsg hash "$bad" 2>&1) || true
    assertEq "the error for $bad quotes the bucket it came from" \
        "$(echo "$out" | grep -c "$oci_bucket")" "1"
done

# The namespace stays optional, and both spellings still reach the same object.
prepare_file addressed.txt "reachable both ways"
oci os object put --region "$oci_region" --namespace "$oci_ns" --bucket-name "$oci_bucket" \
    --file addressed.txt --name "$testid/addressed.txt" --force >/dev/null 2>&1

# Read back rather than merely resolved: the point is that both spellings
# reach the same bytes, not just that both parse.
assertEq "bucket@region reaches the object" \
    "$(../gsg cat "$remote_base/addressed.txt" 2>/dev/null)" "reachable both ways"
assertEq "and so does bucket@namespace.region" \
    "$(../gsg cat "oci://$oci_bucket@$oci_ns.$oci_region/$testid/addressed.txt" 2>/dev/null)" \
    "reachable both ways"

# A real region that is not this bucket's must read as a problem with the
# address, not as an absent object. This is the failure the whole change exists
# to make legible: the bucket is there, just not there.
#
# Which error comes back depends on the tenancy, and both are correct:
#
#   subscribed to the region   -> 404 BucketNotFound  -> "cannot reach bucket"
#   not subscribed             -> 401 NotAuthenticated -> "cannot resolve the namespace"
#
# The second is why that message names the region: the service says nothing
# about regions there, so the bare SDK error reads as a credentials problem.
# What both must do is name the region that was asked for, and neither may
# report the object as simply missing.
if [[ "$oci_region2" != "" && "$oci_region2" != "$oci_region" ]]
then
    elsewhere="$oci_region2"
elif [[ "$oci_region" == "us-ashburn-1" ]]
then
    elsewhere="eu-frankfurt-1"
else
    elsewhere="us-ashburn-1"
fi
out=$(../gsg hash "oci://$oci_bucket@$elsewhere/$testid/addressed.txt" 2>&1) || true
# grep -q, not a line count: the SDK's error is several lines and may mention
# the region in more than one of them.
assertEq "the same bucket named in the wrong region names that region" \
    "$(echo "$out" | grep -q "$elsewhere" && echo named || echo unnamed)" "named"
assertEq "and is not reported as an absent object" \
    "$(echo "$out" | grep -c 'Invalid bucket')" "0"

finish
