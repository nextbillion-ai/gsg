# Lock and unlock. Ground truth is the oci cli and the exit codes.

# oci_receipt <object> -> the /tmp file holding the ETag that proves we hold it.
# The scheme is part of the hash, unlike gs and s3 (TODO item 16).
oci_receipt() {
    echo "/tmp/$(printf '%s' "oci://$oci_bucket/$1" | md5)"
}

start "lock: a round trip, and a second holder is refused"

lk="$testid/lock/a.lock"
rm -f "$(oci_receipt "$lk")"

assertOk "the first lock succeeds" ../gsg lock "oci://$oci_bucket/$lk" 300
assertEq "and the lock object is really there" \
    "$(oci os object head --namespace "$oci_ns" --bucket-name "$oci_bucket" \
        --name "$lk" >/dev/null 2>&1 && echo present || echo absent)" "present"

# The whole point. A second acquisition must fail, not quietly succeed --
# gsg lock returned 0 while doing nothing at all before this backend had any
# locking, which is worse than failing because the caller believes it has
# exclusive access.
if ../gsg lock "oci://$oci_bucket/$lk" 300 >/dev/null 2>&1
then
    echo "FATAL: a second lock succeeded -- the lock is not exclusive"
    exit 1
else
    echo "OK: a second lock is refused"
fi

assertOk "unlock releases it" ../gsg unlock "oci://$oci_bucket/$lk"
assertEq "and the object is gone" \
    "$(oci os object head --namespace "$oci_ns" --bucket-name "$oci_bucket" \
        --name "$lk" >/dev/null 2>&1 && echo present || echo absent)" "absent"
assertOk "so the lock can be taken again" ../gsg lock "oci://$oci_bucket/$lk" 300
../gsg unlock "oci://$oci_bucket/$lk" >/dev/null 2>&1

finish

start "lock: only one of several contenders may take it"

# Eight processes, one object. Before #44 the s3 backend gave eight winners:
# every contender came away believing it held the lock.
rk="$testid/lock/race.lock"
rm -f "$(oci_receipt "$rk")"
i=1
while [[ $i -le 8 ]]
do
    ( ../gsg lock "oci://$oci_bucket/$rk" 300 >/dev/null 2>&1; echo "$?" > "race_$i.rc" ) &
    i=$((i + 1))
done
wait
winners=0
for f in race_*.rc
do
    [[ "$(cat "$f")" == "0" ]] && winners=$((winners + 1))
done
rm -f race_*.rc
assertEq "exactly one of eight contenders wins" "$winners" "1"
../gsg unlock "oci://$oci_bucket/$rk" >/dev/null 2>&1

finish

start "lock: unlock must not release a lock someone else now holds"

# The defect #44 fixed on s3: unlock deleted whichever lock was present, so a
# caller whose own lock had expired would release the one another process had
# since acquired. Both use the same short ttl, because expiry is judged as
# LastModified + the ttl the *acquirer* asks for.
sk="$testid/lock/steal.lock"
rcpt="$(oci_receipt "$sk")"
rm -f "$rcpt"

assertOk "A takes the lock with a one second ttl" ../gsg lock "oci://$oci_bucket/$sk" 1
cp "$rcpt" receiptA
sleep 3
assertOk "B takes over once it has expired" ../gsg lock "oci://$oci_bucket/$sk" 1
assertEq "B's receipt is not A's" \
    "$(cmp -s receiptA "$rcpt" && echo same || echo different)" "different"

# A now tries to unlock with the receipt it still has.
cp "$rcpt" receiptB
cp receiptA "$rcpt"
if ../gsg unlock "oci://$oci_bucket/$sk" >/dev/null 2>&1
then
    echo "FATAL: A released a lock it no longer holds"
    exit 1
else
    echo "OK: A cannot release the lock B now holds"
fi
assertEq "and B's lock survived" \
    "$(oci os object head --namespace "$oci_ns" --bucket-name "$oci_bucket" \
        --name "$sk" >/dev/null 2>&1 && echo present || echo absent)" "present"

cp receiptB "$rcpt"
assertOk "B can still release its own" ../gsg unlock "oci://$oci_bucket/$sk"
rm -f receiptA receiptB

finish

start "lock: unlock without a receipt says so, and releases nothing"

# A receipt names the ETag that proves the lock is ours. Without it there is
# nothing that could safely be deleted, so nothing is -- and the lock stands.
# The exit code is 0, matching gs, s3 and linux, which all treat a missing
# receipt as nothing to do. This case pins both halves, because a 0 on its own
# would read as "the lock is gone".
nk="$testid/lock/norcpt.lock"
nrcpt="$(oci_receipt "$nk")"
rm -f "$nrcpt"
assertOk "take the lock" ../gsg lock "oci://$oci_bucket/$nk" 300
rm -f "$nrcpt"

out=$(../gsg unlock "oci://$oci_bucket/$nk" 2>&1) || true
assertOk "unlock without a receipt still exits 0" ../gsg unlock "oci://$oci_bucket/$nk"
assertEq "it says nothing was released" \
    "$(echo "$out" | grep -c 'nothing was released')" "1"
assertEq "and the lock is still held" \
    "$(oci os object head --namespace "$oci_ns" --bucket-name "$oci_bucket" \
        --name "$nk" >/dev/null 2>&1 && echo present || echo absent)" "present"

# Clean up: only the ETag can release it, so take it back the long way.
oci os object delete --namespace "$oci_ns" --bucket-name "$oci_bucket" \
    --name "$nk" --force >/dev/null 2>&1

finish
