# Proves the oci) branches of the shared assert helpers actually work.
#
# It deliberately uses the oci cli rather than gsg: at this point the backend
# is a skeleton and can do nothing, but the helpers every later case file
# depends on still need to be known-good. Once real operations land this case
# stays useful as the control -- if it fails, the harness is broken rather than
# the backend.
#
# Case files are sourced from inside $testbase with $mode, $remote_base,
# $testid, $oci_ns and $oci_bucket already set.

start "harness: the oci assert helpers agree with the bucket"

prepare_file harness.txt
oci os object put --namespace "$oci_ns" --bucket-name "$oci_bucket" \
    --file harness.txt --name "$testid/harness.txt" --force >/dev/null 2>&1

# assert checks existence and that the content is $testid, which prepare_file wrote.
assert harness.txt remote

# assertValue checks existence and an explicit expected value.
prepare_file valued.txt "a specific value"
oci os object put --namespace "$oci_ns" --bucket-name "$oci_bucket" \
    --file valued.txt --name "$testid/valued.txt" --force >/dev/null 2>&1
assertValue valued.txt "a specific value" remote

# assert_not must be satisfied by a key that was never written, and by one that
# was written and then removed -- the second is the case that matters, since a
# helper that always says "not there" would pass the first.
assert_not never-written.txt remote
oci os object delete --namespace "$oci_ns" --bucket-name "$oci_bucket" \
    --name "$testid/valued.txt" --force >/dev/null 2>&1
assert_not valued.txt remote

finish
