set -e
testid="$(date +%s)-$$"
remote_base_template="://gsg-uat/$testid"
start() {
    echo ">>>>>>>> $1"
}

finish() {
    what=$1
    if [[ "$what" == "" ]]
    then
        what="done"
    fi
    echo "<<<<<<<< $what"
    echo
}

same() {
    if diff $1 $2 &>/dev/null
    then
        echo OK: $1 and $2 does not differ
    else
        echo FATAL: $1 and $2 are different
        exit 1
    fi
}

assertValue() {
    if [[ "$3" == "remote" ]]
    then
        case $mode in
        gs)
            if gsutil ls "$remote_base/$1" &>/dev/null
            then
                content="$(gsutil cat "$remote_base/$1" 2>/dev/null)"
                if [[ "$content" == "$2" ]]
                then
                    echo OK: $1 exists with correct content remotely.
                else
                    echo FATAL: required file $1 does not have correct content remotely.
                    exit 1
                fi
            else
                echo FATAL: required file $1 does not exists remotely.
                exit 1
            fi
            ;;
        s3)
            if aws s3 cp "$remote_base/$1" .temp &>/dev/null
            then
                content="$(cat .temp)"
                if [[ "$content" == "$2" ]]
                then
                    echo OK: $1 exists with correct content remotely.
                else
                    echo FATAL: required file $1 does not have correct content remotely.
                    exit 1
                fi
            else
                echo FATAL: required file $1 does not exists remotely.
                exit 1
            fi
            ;;
        *)
            exit 1
            ;;
        esac
    else
        if ls "$1" &>/dev/null
        then
            content=$(cat "$1")
            if [[ "$content" == "$2" ]]
            then
                echo OK: $1 exists with correct content locally.
            else
                echo FATAL: required file $1 does not have correct content locally.
                exit 1
            fi
        else
            echo FATAL: required file $1 does not exists locally.
            exit 1
        fi
    fi
}

assert() {
    if [[ "$2" == "remote" ]]
    then
        case $mode in
        gs)
            if gsutil ls "$remote_base/$1" &>/dev/null
            then
                content="$(gsutil cat "$remote_base/$1" 2>/dev/null)"
                if [[ "$content" == "$testid" ]]
                then
                    echo OK: $1 exists with correct content remotely.
                else
                    echo FATAL: required file $1 does not have correct content remotely.
                    exit 1
                fi
            else
                echo FATAL: required file $1 does not exists remotely.
                exit 1
            fi
            ;;
        s3)
            if aws s3 cp "$remote_base/$1" .temp &>/dev/null
            then
                content="$(cat .temp)"
                if [[ "$content" == "$testid" ]]
                then
                    echo OK: $1 exists with correct content remotely.
                else
                    echo FATAL: required file $1 does not have correct content remotely.
                    exit 1
                fi
            else
                echo FATAL: required file $1 does not exists remotely.
                exit 1
            fi
            ;;
        *)
            exit 1
            ;;
        esac
    else
        if ls "$1" &>/dev/null
        then
            content=$(cat "$1")
            if [[ "$content" == "$testid" ]]
            then
                echo OK: $1 exists with correct content locally.
            else
                echo FATAL: required file $1 does not have correct content locally.
                exit 1
            fi
        else
            echo FATAL: required file $1 does not exists locally.
            exit 1
        fi
    fi
}

assert_not() {
    if [[ "$2" == "remote" ]]
    then
        case $mode in
        gs)
            if gsutil ls "$remote_base/$1" &>/dev/null
            then
                echo FATAL: required file $1 does exists remotely.
                exit 1
            else
                echo OK: required file $1 does not exists remotely.
            fi
            ;;
        s3)
            if aws s3 cp "$remote_base/$1" .temp &>/dev/null
            then
                echo FATAL: required file $1 does exists remotely.
                exit 1
            else
                echo OK: required file $1 does not exists remotely.
            fi
            ;;
        *)
            exit 1
            ;;
        esac
    else
        if ls "$1" &>/dev/null
        then
            echo FATAL: required file $1 does exists locally.
            exit 1
        else
            echo OK: required file $1 does not exists locally.
        fi
    fi
}

remote_copy() {
    local r=""
    case $mode in
    gs)
        if [[ "$1" == "true" ]]
        then
            r=" -r"
        fi
        echo "gsutil cp $r"
        ;;
    s3)
        if [[ "$1" == "true" ]]
        then
            r=" --recursive"
        fi
        echo "aws s3 cp $r"
        ;;
    *)
        exit 1
        ;;
    esac
}

prepare_file() {
    value=$testid
    if [[ "$2" != "" ]]
    then
        value=$2
    fi
    echo "$value" > "$1"
}

# assertEq compares a value against an expectation and reports which check failed.
assertEq() {
    local what="$1" got="$2" want="$3"
    if [[ "$got" == "$want" ]]
    then
        echo "OK: $what"
    else
        echo "FATAL: $what"
        echo "      want: [$want]"
        echo "      got:  [$got]"
        exit 1
    fi
}

# assertOk runs a command and requires it to succeed. Several of the bugs these
# cases cover were panics, so a non-zero exit is the failure being checked for.
assertOk() {
    local what="$1"; shift
    if "$@" >/dev/null 2>&1
    then
        echo "OK: $what"
    else
        echo "FATAL: $what -- command exited $? : $*"
        exit 1
    fi
}

# assertNoCrash runs a command that is allowed to fail, and requires only that
# it did not crash. Some cases below cover panics where a clean error is a
# perfectly good outcome, and plain "cmd || true" would hide the panic exactly
# as well as it hides the error.
assertNoCrash() {
    local what="$1"; shift
    local out
    out="$("$@" 2>&1)" || true
    # DATA RACE belongs here too: under GSG_UAT_RACE the detector prints that
    # and exits non-zero, and the "|| true" above would otherwise let a race
    # pass as a clean failure.
    local bad="panic:|index out of range|slice bounds out of range|\[RECOVERED\]|WARNING: DATA RACE"
    if echo "$out" | grep -qE "$bad"
    then
        echo "FATAL: $what -- crashed"
        echo "$out" | grep -E "$bad" | head -3 | sed 's/^/      /'
        exit 1
    fi
    echo "OK: $what"
}

# snapshotTmp / poisonNewTmp reproduce the state a killed run leaves behind.
# gsg caches crc32c values and lock generations in /tmp under md5 names and
# used to decode them without checking their length. The exact names depend on
# a file's mtime formatted by Go, which the shell cannot reproduce, so the new
# ones are found by diffing /tmp -- but only files that are both named like an
# md5 and exactly the size of the cache being targeted are touched. That keeps
# an unrelated process's temp file safe, and stops a stray file from standing
# in for the cache we meant to corrupt and giving a false pass.
snapshotTmp() {
    ls /tmp > .tmp_before 2>/dev/null || true
}

# poisonNewTmp <expected-size-in-bytes>
poisonNewTmp() {
    local want="$1" n=0 f
    ls /tmp > .tmp_after 2>/dev/null || true
    for f in $(comm -13 .tmp_before .tmp_after 2>/dev/null | grep -E '^[0-9a-f]{32}$')
    do
        if [[ -f "/tmp/$f" && "$(wc -c < "/tmp/$f" | tr -d ' ')" == "$want" ]]
        then
            : > "/tmp/$f"
            n=$((n+1))
        fi
    done
    echo "$n"
}

do_test() {
    mode=$1
    remote_base="$1$remote_base_template"

        # A failing assertion exits immediately, which skips the cleanup below and
    # is usually what you want -- the remote state is what you need to look at.
    # Say where it is, since the prefix is timestamped.
    trap 'code=$?; if [[ $code -ne 0 ]]; then
        echo
        echo "test data left behind for inspection at: $remote_base"
        case $mode in
        gs) echo "remove it with: gsutil -m rm -r $remote_base" ;;
        s3) echo "remove it with: aws s3 rm $remote_base --recursive" ;;
        esac
    fi' EXIT

    start "prepare test ground for mode: $mode"
    rm -rf uat_temp || true
    testbase="uat_temp"
    mkdir $testbase
    finish

    start "entering $testbase"
    pushd $testbase
    finish

    start "test upload"
    start "test upload single file"
    prepare_file to_upload
    ../gsg cp to_upload $remote_base/to_upload
    assert to_upload remote
    finish
    ftu="folder_to_upload"

    start "test upload a folder"
    mkdir -p $ftu/a/b/c
    prepare_file $ftu/a/1.txt 1_txt
    prepare_file $ftu/a/2.txt 2_txt
    prepare_file $ftu/a/b/c/3.txt 3_txt
    ../gsg -m cp -r $ftu $remote_base/$ftu
    assertValue $ftu/a/1.txt 1_txt remote
    assertValue $ftu/a/2.txt 2_txt remote
    assertValue $ftu/a/b/c/3.txt 3_txt remote
    finish

    start "test download"

    start "test download single file"
    prepare_file to_download
    $(remote_copy) to_download $remote_base/to_download
    rm to_download
    ../gsg cp $remote_base/to_download to_download
    assert to_download
    finish

    ftd="folder_to_download"
    start "test download a folder"
    mkdir -p $ftd/a/b/c
    prepare_file $ftd/a/1.txt 1_txt
    prepare_file $ftd/a/2.txt 2_txt
    prepare_file $ftd/a/b/c/3.txt 3_txt
    $(remote_copy true) $ftd $remote_base/$ftd
    rm -rf $ftd
    ../gsg -m cp -r $remote_base/$ftd $ftd
    assertValue $ftd/a/1.txt 1_txt
    assertValue $ftd/a/2.txt 2_txt
    assertValue $ftd/a/b/c/3.txt 3_txt
    finish

    ftm="folder_to_move"
    start "test moving a folder"
    mkdir -p $ftm/a/b/c
    prepare_file $ftm/a/1.txt
    prepare_file $ftm/a/2.txt
    prepare_file $ftm/a/b/c/3.txt
    $(remote_copy true) $ftm $remote_base/$ftm
    rm -rf $ftm
    ../gsg -m mv -r $remote_base/$ftm $ftm
    assert $ftm/a/1.txt
    assert $ftm/a/2.txt
    assert $ftm/a/b/c/3.txt
    assert_not $ftm/a/1.txt remote
    assert_not $ftm/a/2.txt remote
    assert_not $ftm/a/b/c/3.txt remote
    finish


    start "testing rsync"
    ftr="folder_to_rsync"

    start "test rsync a local folder to remote"
    mkdir -p $ftr/a/b/c
    prepare_file $ftr/a/1.txt
    prepare_file $ftr/a/2.txt
    prepare_file $ftr/a/b/c/3.txt
    ../gsg -m rsync -r $ftr $remote_base/$ftr
    assert $ftr/a/1.txt remote
    assert $ftr/a/2.txt remote
    assert $ftr/a/b/c/3.txt remote
    echo "whocares" > $ftr/a/1.txt
    ../gsg -m rsync -r $ftr $remote_base/$ftr
    $(remote_copy) $remote_base/$ftr/a/1.txt $ftr/a/1_remote.txt
    same $ftr/a/1.txt $ftr/a/1_remote.txt
    finish

    start "test rsync a remote folder to local"
    prepare_file $ftr/a/1.txt
    $(remote_copy) $ftr/a/1.txt $remote_base/$ftr/a/1.txt
    rm -rf $ftr
    ../gsg -m rsync -r $remote_base/$ftr $ftr
    assert $ftr/a/1.txt
    assert $ftr/a/2.txt
    assert $ftr/a/b/c/3.txt
    finish

    start "test rsync with -d and non-existing src"
    ../gsg -m rsync -d whocares $remote_base/$ftr
    assert_not $ftr/a/1.txt remote
    assert_not $ftr/a/2.txt remote
    assert_not $ftr/a/b/c/3.txt remote
    finish


    # ---------------------------------------------------------------------
    # Regression cases.
    #
    # Everything above exercises cp, mv and rsync. du, ls, cat, rm and the -v
    # checksum path had no coverage at all, which is where a set of crashes
    # went unnoticed. Each case below fails on the commit that introduced it.
    # ---------------------------------------------------------------------

    start "regression: the dropped directory level, isolated from the panic"
    # Deliberately before the case below. That one has a file directly under
    # the prefix, which makes the unfixed code panic before any assertion runs,
    # so it can only prove the final behaviour -- never that dirs[1:] ALSO
    # dropped a level. Nothing sits directly under this prefix, so the unfixed
    # code gets through it without panicking:
    # GetAllParents returns two entries, dirs[1:] keeps only the deeper one,
    # and the x/ level silently disappears.
    fnest="folder_nested_only"
    mkdir -p $fnest/x/y
    printf '01234567' > $fnest/x/y/only.txt   # 8 bytes, nothing at the top level
    ../gsg -m cp -r $fnest $remote_base/$fnest
    assertOk "du exits cleanly with no direct child" ../gsg du $remote_base/$fnest
    assertEq "du reports the x/ level"   "$(../gsg du $remote_base/$fnest 2>/dev/null | grep -c "/$fnest/x/$")" "1"
    assertEq "du reports the x/y/ level" "$(../gsg du $remote_base/$fnest 2>/dev/null | grep -c "/$fnest/x/y/$")" "1"
    assertEq "du -s total"               "$(../gsg du -s $remote_base/$fnest 2>/dev/null | awk '{print $1}')" "8"
    assertEq "x/ subtotal"               "$(../gsg du $remote_base/$fnest 2>/dev/null | grep "/$fnest/x/$" | awk '{print $1}')" "8"
    finish

    start "regression: du over a prefix with files directly under it"
    # DiskUsage walked the parent chain with dirs[1:], and GetAllParents
    # returns an empty slice for an object sitting directly under the prefix,
    # so this panicked with "slice bounds out of range [1:0]". Only trees whose
    # every object was nested survived, which made it look intermittent.
    fdu="folder_to_du"
    mkdir -p $fdu/sub/deep
    printf '0123456789'   > $fdu/direct.txt       # 10 bytes, directly under the prefix
    printf '01234'        > $fdu/sub/mid.txt      # 5
    printf '0123456789ab' > $fdu/sub/deep/low.txt # 12
    ../gsg -m cp -r $fdu $remote_base/$fdu
    assertOk "du -s exits cleanly" ../gsg du -s $remote_base/$fdu
    assertOk "du exits cleanly"    ../gsg du    $remote_base/$fdu
    assertEq "du -s totals every object" \
        "$(../gsg du -s $remote_base/$fdu 2>/dev/null | awk '{print $1}')" "27"
    # dirs[1:] also dropped the shallowest directory between prefix and object,
    # so a whole level went missing from the listing even when it did not panic.
    assertEq "du reports the sub/ level"      "$(../gsg du $remote_base/$fdu 2>/dev/null | grep -c "/$fdu/sub/$")" "1"
    assertEq "du reports the sub/deep/ level" "$(../gsg du $remote_base/$fdu 2>/dev/null | grep -c "/$fdu/sub/deep/$")" "1"
    assertEq "du subtotal for sub/" \
        "$(../gsg du $remote_base/$fdu 2>/dev/null | grep "/$fdu/sub/$" | awk '{print $1}')" "17"
    assertEq "du subtotal for sub/deep/" \
        "$(../gsg du $remote_base/$fdu 2>/dev/null | grep "/$fdu/sub/deep/$" | awk '{print $1}')" "12"
    assertEq "du -s lists exactly one row" \
        "$(../gsg du -s $remote_base/$fdu 2>/dev/null | wc -l | tr -d ' ')" "1"
    # The exact set, not just the count -- six rows of the wrong six would pass
    # a cardinality check.
    assertEq "du lists exactly these paths" \
        "$(../gsg du $remote_base/$fdu 2>/dev/null | awk '{print $2}' | sed "s#^.*/$testid/##" | sort | tr '\n' ' ')" \
        "$fdu/ $fdu/direct.txt $fdu/sub/ $fdu/sub/deep/ $fdu/sub/deep/low.txt $fdu/sub/mid.txt "
    finish


    start "regression: du -h and -sh, the flag the original regression was named after"
    # d5c1e42 "fix: du -sh" is the commit that turned the tree walk into a
    # panic, and nothing here ever ran the human readable path.
    assertOk "du -sh exits cleanly" ../gsg du -sh $remote_base/$fdu
    assertEq "du -sh renders the total" \
        "$(../gsg du -sh $remote_base/$fdu 2>/dev/null | awk '{print $1, $2}')" "27 B"
    printf '%2048s' '' > kib.bin
    ../gsg cp kib.bin $remote_base/$fdu/kib.bin
    assertEq "du -sh renders KiB" \
        "$(../gsg du -sh $remote_base/$fdu/kib.bin 2>/dev/null | awk '{print $1, $2}')" "2.00 KiB"
    assertEq "du -h keeps the per-directory rows" \
        "$(../gsg du -h $remote_base/$fdu 2>/dev/null | grep -c "/$fdu/sub/$")" "1"
    ../gsg rm $remote_base/$fdu/kib.bin >/dev/null 2>&1 || true
    finish

    start "regression: a directory marker object must not be listed twice"
    # An object whose key ends in "/" -- what console UIs and Hadoop write to
    # fake a folder. GetAllParents already ends with that directory, so the
    # tree walk lands on its node; adding a leaf for it as well emitted the
    # same name twice. Covered by unit tests in system/tree_test.go; this is
    # the end to end check.
    #
    # s3 only: aws s3api can put a key ending in "/", gsutil cannot -- it
    # appends the local filename instead. The branch being exercised lives in
    # system/tree.go and is shared by both backends.
    if [[ "$mode" != "s3" ]]
    then
        echo "SKIP: gsutil cannot create an object whose key ends in /"
        finish
    else
    # The marker carries bytes on purpose. Their fate pins a second, separate
    # behaviour, described below.
    printf '1234567' > marker.bin   # 7 bytes
    aws s3api put-object --bucket gsg-uat --key "$testid/$fdu/sub/" --body marker.bin >/dev/null
    assertEq "du lists sub/ exactly once, not twice" \
        "$(../gsg du $remote_base/$fdu 2>/dev/null | grep -c "/$fdu/sub/$")" "1"
    # 27, not 34: batchAttrs short-circuits every key ending in "/" and
    # synthesizes an empty GetObjectAttributesOutput for it, so the marker's
    # size is never read and its 7 bytes go uncounted. Right for a common
    # prefix, which has no size; wrong for a real object that happens to end in
    # "/". Pre-existing, unchanged by #35, and recorded in TODO.md -- pinned
    # here so that fixing it is a deliberate act rather than a surprise.
    assertEq "a marker's bytes are currently NOT counted (known, see TODO.md)" \
        "$(../gsg du -s $remote_base/$fdu 2>/dev/null | awk '{print $1}')" "27"
    assertEq "and the sub/ subtotal is unchanged by it" \
        "$(../gsg du $remote_base/$fdu 2>/dev/null | grep "/$fdu/sub/$" | awk '{print $1}')" "17"
    aws s3api delete-object --bucket gsg-uat --key "$testid/$fdu/sub/" >/dev/null
    finish
    fi

    start "regression: du -s on a single object and on a bucket prefix"
    assertOk "du -s of one object" ../gsg du -s $remote_base/$fdu/direct.txt
    assertEq "du -s of one object reports its size" \
        "$(../gsg du -s $remote_base/$fdu/direct.txt 2>/dev/null | awk '{print $1}')" "10"
    finish

    # Not covered here: s3 batchAttrs leaves a nil entry when an object's
    # attribute lookup fails, and List and DiskUsage must skip those. Making a
    # lookup fail on demand is not something a live harness can arrange, so
    # that path is left to the Go tests.

    start "regression: ls -r returns exactly the objects that exist"
    assertEq "ls -r lists all three" \
        "$(../gsg ls -r $remote_base/$fdu 2>/dev/null | sed "s#.*/$fdu/##" | sort | tr '\n' ' ')" \
        "direct.txt sub/deep/low.txt sub/mid.txt "
    assertEq "ls -l reports the byte size" \
        "$(../gsg ls -l $remote_base/$fdu/direct.txt 2>/dev/null | awk '{print $1}')" "10"
    finish

    start "regression: a non-recursive listing mixes objects and common prefixes"
    # Every other listing here is recursive. Without -r the provider is asked
    # for a delimited listing, which returns real objects alongside synthetic
    # "common prefix" entries for the subdirectories. The two are handled by
    # different branches of the s3 backend's batchAttrs -- a common prefix
    # needs no attribute request -- and nothing exercised that mix.
    assertEq "ls without -r returns this level plus the subdirectory" \
        "$(../gsg ls $remote_base/$fdu 2>/dev/null | sed "s#.*/$fdu/##" | sort | tr '\n' ' ')" \
        "direct.txt sub/ "
    assertEq "ls -r still returns the leaves, not the subdirectory" \
        "$(../gsg ls -r $remote_base/$fdu 2>/dev/null | grep -c "/$fdu/sub/\$")" "0"
    finish

    start "regression: a listing larger than the attribute fan-out limit"
    # batchAttrs caps concurrent attribute requests at 64. Every other fixture
    # here is small enough that the cap never engages, so nothing covered the
    # path where work queues behind it.
    fmany="folder_many"
    mkdir -p $fmany
    i=1
    while [[ $i -le 70 ]]
    do
        printf '%s' "$i" > $fmany/f$i.txt
        i=$((i + 1))
    done
    ../gsg -m cp -r $fmany $remote_base/$fmany
    assertEq "all 70 objects list back" \
        "$(../gsg ls -r $remote_base/$fmany 2>/dev/null | wc -l | tr -d ' ')" "70"
    assertEq "and du -s totals them" \
        "$(../gsg du -s $remote_base/$fmany 2>/dev/null | awk '{print $1}')" \
        "$(cat $fmany/* | wc -c | tr -d ' ')"
    finish

    start "regression: cat returns exact content"
    assertEq "cat direct.txt" "$(../gsg cat $remote_base/$fdu/direct.txt 2>/dev/null)" "0123456789"
    finish

    start "regression: a truncated crc32c cache must not crash the next run"
    # The cache write was not atomic: the file was created, then the four bytes
    # were written. A run killed in between left an empty file in /tmp for good,
    # and every later run decoded it as a uint32 and panicked with
    # "index out of range [3] with length 0".
    #
    # gs only. S3.Download never verifies the downloaded file: it takes
    # forceChecksum but uses it solely to set ChecksumMode on the GetObject
    # request, and S3.MustEqualCRC32C is defined and never called from
    # anywhere. So -v computes no checksum on the s3 path and writes no cache
    # for this case to corrupt. Enable this for s3 once that is fixed.
    if [[ "$mode" != "gs" ]]
    then
        echo "SKIP: s3 downloads do not verify checksums, so -v writes no crc32c cache"
        finish
    else
    fcrc="folder_crc"
    mkdir -p $fcrc
    prepare_file $fcrc/one.txt crc_one
    prepare_file $fcrc/two.txt crc_two
    ../gsg -m cp -r $fcrc $remote_base/$fcrc
    rm -rf ${fcrc}_down && mkdir -p ${fcrc}_down
    snapshotTmp
    ../gsg -m cp -r -v $remote_base/$fcrc ${fcrc}_down
    poisoned=$(poisonNewTmp 4)   # a crc32c cache is exactly 4 bytes
    if [[ "$poisoned" == "0" ]]
    then
        echo "FATAL: expected the -v download to leave crc32c cache files in /tmp, found none"
        exit 1
    fi
    echo "OK: emptied $poisoned crc32c cache file(s) to mimic a run killed mid-write"
    assertOk "cp -v survives a truncated crc32c cache" \
        ../gsg -m cp -r -v $remote_base/$fcrc ${fcrc}_down
    assertValue ${fcrc}_down/one.txt crc_one
    snapshotTmp
    ../gsg -m rsync -r -v $remote_base/$fcrc ${fcrc}_down
    poisonNewTmp 4 >/dev/null
    assertOk "rsync -v survives a truncated crc32c cache" \
        ../gsg -m rsync -r -v $remote_base/$fcrc ${fcrc}_down
    finish
    fi

    start "regression: a truncated lock cache must not crash unlock"
    # Same shape as the crc32c cache: the generation was decoded from /tmp with
    # no length check, so a short file panicked with "index out of range".
    #
    # gs only. The s3 backend caches the lock's ETag as a variable length
    # string and never decodes a fixed width value from it, so it has no such
    # panic and nothing of a known size for poisonNewTmp to target.
    if [[ "$mode" != "gs" ]]
    then
        echo "SKIP: s3 caches an etag string, so it has no fixed width decode to corrupt"
        finish
    else
    snapshotTmp
    ../gsg lock $remote_base/lockfile 3600
    poisoned=$(poisonNewTmp 8)   # a gs lock generation is 8 bytes
    if [[ "$poisoned" == "0" ]]
    then
        echo "FATAL: expected lock to leave an 8 byte generation cache in /tmp, found none"
        exit 1
    fi
    echo "OK: emptied $poisoned lock cache file(s)"
    # Unlock cannot succeed without the generation, so a clean failure is a
    # fine outcome here -- but it must not panic. "|| true" would hide the
    # panic just as well as the error, so the output is inspected instead.
    assertNoCrash "unlock does not crash on a truncated lock cache" \
        ../gsg unlock $remote_base/lockfile

    # Not crashing is not enough. Without the generation the remote lock cannot
    # be released, so unlock has to say so: reporting success would leave the
    # next locker blocked until the TTL with nobody aware of why.
    if ../gsg unlock $remote_base/lockfile >/dev/null 2>&1
    then
        echo "FATAL: unlock reported success despite an unusable lock cache"
        exit 1
    fi
    echo "OK: unlock reports the failure rather than claiming success"

    # And the claim is accurate: the lock really is still held.
    assertValue lockfile 1 remote

    assertOk "gsg still runs afterwards" ../gsg ls -r $remote_base/$fdu
    ../gsg rm $remote_base/lockfile >/dev/null 2>&1 || true
    finish
    fi

    start "regression: unlock must not release a lock someone else now holds"
    # A takes the lock and caches its receipt. A's lock then goes away and B
    # takes a new one at the same path -- a different generation on gs, a
    # different ETag on s3. A's unlock has to refuse: its receipt names the old
    # object, not B's.
    #
    # gs conditioned its delete on the stored generation from the start. s3
    # took an etag argument, logged it, and deleted unconditionally, so A's
    # unlock removed B's lock and reported success. Both backends are checked
    # here so the two cannot drift again.
    aunl="unlock_other.lock"
    ../gsg lock $remote_base/$aunl 3600            # A locks, receipt cached locally
    case $mode in
    gs)
        gsutil rm "$remote_base/$aunl" >/dev/null 2>&1
        echo "B" | gsutil cp - "$remote_base/$aunl" >/dev/null 2>&1
        ;;
    s3)
        aws s3api delete-object --bucket gsg-uat --key "$testid/$aunl" >/dev/null 2>&1
        printf 'B' | aws s3 cp - "$remote_base/$aunl" >/dev/null 2>&1
        ;;
    esac
    if ../gsg unlock $remote_base/$aunl >/dev/null 2>&1
    then
        echo "FATAL: unlock reported success on a lock held by someone else"
        exit 1
    fi
    echo "OK: unlock refuses a lock it does not hold"
    assertValue $aunl B remote
    case $mode in
    gs) gsutil rm "$remote_base/$aunl" >/dev/null 2>&1 ;;
    s3) aws s3api delete-object --bucket gsg-uat --key "$testid/$aunl" >/dev/null 2>&1 ;;
    esac
    finish

    start "lock and unlock round trip"
    # After the case above the cache file holds a valid generation again, so a
    # normal round trip must still work. Runs second because that case needs
    # the lock cache to be newly created in order to find and corrupt it, and
    # gs leaves the file behind after a successful unlock.
    assertOk "lock succeeds"   ../gsg lock $remote_base/lockfile 3600
    assertOk "unlock succeeds" ../gsg unlock $remote_base/lockfile
    assert_not lockfile remote
    finish

    start "regression: filenames with spaces and non-ascii characters"
    fodd="folder_odd"
    mkdir -p $fodd
    prepare_file "$fodd/with spaces.txt" spaced
    prepare_file "$fodd/café-日.txt" accented
    ../gsg -m cp -r $fodd $remote_base/$fodd
    # Checked with gsutil/aws rather than gsg ls, so a path bug shared by both
    # sides of gsg cannot make this pass.
    assertValue "$fodd/with spaces.txt" spaced remote
    assertValue "$fodd/café-日.txt" accented remote
    rm -rf ${fodd}_down && mkdir -p ${fodd}_down
    ../gsg -m cp -r $remote_base/$fodd ${fodd}_down
    assertOk "odd names round-trip identically" diff -r $fodd ${fodd}_down
    finish

    start "regression: rm removes exactly what it is asked to"
    before=$(../gsg ls -r $remote_base/$fdu 2>/dev/null | wc -l | tr -d ' ')
    ../gsg rm $remote_base/$fdu/direct.txt
    assertEq "rm removed one object" \
        "$(../gsg ls -r $remote_base/$fdu 2>/dev/null | wc -l | tr -d ' ')" "$((before-1))"
    assert_not $fdu/direct.txt remote
    assertValue $fdu/sub/mid.txt 01234 remote
    assertValue $fdu/sub/deep/low.txt 0123456789ab remote
    finish

    start "regression: local listing and du survive awkward filenames"
    # Everything above lists remote prefixes. The local backend shells out to
    # find and du and parses their text output, which is where a separate set
    # of bugs lived, and nothing here exercised it directly.
    flocal="folder_local"
    nl=$'\n'
    mkdir -p $flocal/sub
    prepare_file "$flocal/normal.txt" plain
    prepare_file "$flocal/with spaces.txt" spaced
    prepare_file "$flocal/sub/nested.txt" nested
    printf 'x' > "$flocal/trailing "          # basename ENDS in a space
    printf 'y' > "$flocal/we${nl}ird.txt"     # newline inside the name

    # find's output was split on newlines, so the name above became two paths
    # naming nothing. Those reached callers with no attributes, and rsync
    # dereferenced them. The trailing space was corrupted the same way, by a
    # strings.Trim over the whole path.
    assertOk "ls -r exits cleanly" ../gsg ls -r $flocal
    assertEq "ls -r keeps the trailing space" \
        "$(../gsg ls -r $flocal 2>/dev/null | grep -c "/trailing \$")" "1"
    # Compared after turning newlines into \001, and matched against \001 -- a
    # newline inside a grep pattern separates alternatives, so grepping for
    # "we<newline>ird.txt" would really just be grepping for "we", which the
    # broken output contains too.
    assertEq "ls -r keeps the newline name in one piece" \
        "$(../gsg ls -r $flocal 2>/dev/null | tr '\n' '\001' | grep -c "we"$'\001'"ird.txt")" "1"

    # du shells out to "du -aB1" and split its output on newlines too. The
    # newline name produces a continuation line carrying no size, and reading
    # its second field panicked with "index out of range [1] with length 1".
    assertOk "du -s exits cleanly over those names" ../gsg du -s $flocal
    assertEq "du -s agrees with the system du" \
        "$(../gsg du -s $flocal 2>/dev/null | awk '{print $1}')" \
        "$(du -sB1 $flocal 2>/dev/null | awk '{print $1}')"

    # A local round trip: the trailing-space name alone used to abort this with
    # a nil dereference, syncing nothing at all.
    rm -rf ${flocal}_copy
    ../gsg -m rsync -r $flocal ${flocal}_copy
    assertOk "a local rsync reproduces the tree exactly" diff -r $flocal ${flocal}_copy
    finish

    start "regression: an unreadable subdirectory must fail, not list empty"
    # find exits non-zero for one unreadable subdirectory while still printing
    # what it found, and List discarded the lot and reported success with an
    # empty result. rsync -d deletes whatever the source listing omits, so a
    # permission error under the source could empty the destination.
    if [[ "$(id -u)" == "0" ]]
    then
        echo "SKIP: running as root, which can read the directory regardless of its mode"
        finish
    else
    # Exit status alone proves nothing here: the unfixed code also exits
    # non-zero, because its empty listing makes ls print "No objects found"
    # and quit. What distinguishes them is what -d does to the destination.
    mkdir -p $flocal/denied
    prepare_file $flocal/denied/hidden.txt hidden
    chmod 000 $flocal/denied
    ../gsg ls -r $flocal > .lsout 2>&1 || true
    chmod 755 $flocal/denied
    if grep -q "No objects found" .lsout
    then
        echo "FATAL: an unreadable subdirectory was reported as an empty listing"
        exit 1
    fi
    echo "OK: an unreadable subdirectory is reported as a failure, not as empty"

    # The destination must survive. -d deletes whatever the source listing
    # omits, and an empty listing omits everything: against the unfixed code
    # this exits 0 and removes every file in the destination.
    chmod 000 $flocal/denied
    ../gsg -m rsync -r -d $flocal ${flocal}_copy > .rsout 2>&1 || true
    chmod 755 $flocal/denied
    assertValue ${flocal}_copy/normal.txt plain
    assertValue "${flocal}_copy/with spaces.txt" spaced
    assertValue ${flocal}_copy/sub/nested.txt nested
    rm -f .lsout .rsout
    finish
    fi

    start "rm -r removes a whole tree"
    # cmd/rm.go schedules recursive deletes through the worker pool, and
    # nothing here exercised that at all.
    frm="folder_to_rm"
    mkdir -p $frm/a/b
    prepare_file $frm/a/1.txt
    prepare_file $frm/a/b/2.txt
    ../gsg -m cp -r $frm $remote_base/$frm
    assert $frm/a/1.txt remote
    ../gsg -m rm -r $remote_base/$frm
    assert_not $frm/a/1.txt remote
    assert_not $frm/a/b/2.txt remote
    finish

    start "leaving $testbase"
    popd
    finish

    start "cleanup test ground"
    # The /tmp snapshots live inside uat_temp, so this covers them.
    rm -rf uat_temp || true
    # Only this run's own prefix. This used to delete every object in the
    # bucket, which is fine on a scratch bucket and destructive anywhere else.
    cleaned=true
    case $mode in
    gs)
        gsutil -m rm -r "$remote_base" || cleaned=false
        ;;
    s3)
        aws s3 rm "$remote_base" --recursive || cleaned=false
        ;;
    esac
    # Non-fatal, but never silent: reporting success while leaving objects in
    # the bucket is how a scratch bucket quietly fills up.
    if [[ "$cleaned" != "true" ]]
    then
        echo "WARNING: could not remove $remote_base -- objects are still there"
    fi

    finish

    trap - EXIT
    finish "everything OK with $mode !"
}

usage() {
    echo "usage: $0 [gs|s3|all]    (default: all)"
    echo
    echo "  gs   requires gsutil and GOOGLE_APPLICATION_CREDENTIALS"
    echo "  s3   requires the aws cli and its credentials"
    echo
    echo "Objects are written under <scheme>://gsg-uat/<timestamp> and that"
    echo "prefix is removed at the end."
    echo
    echo "GSG_UAT_RACE=1 builds gsg with the race detector and aborts on any"
    echo "data race. The bulk operations below run with -m, so this genuinely"
    echo "exercises the worker pool. Expect it to fail today: gcs.Init and"
    echo "s3.Init race on their lazy client, which is TODO.md item 12."
    exit 1
}

requireTool() {
    if ! command -v "$1" >/dev/null 2>&1
    then
        echo "FATAL: $1 is required for mode $2 but is not installed"
        exit 1
    fi
}

target="${1:-all}"
case "$target" in
gs|s3|all) ;;
*) usage ;;
esac

start "building gsg binary"
# GSG_UAT_RACE=1 builds with the race detector and makes any data race abort
# the run. Off by default because it is 2-10x slower, and because gsg currently
# has known races that would stop the suite before it tests anything -- see the
# note in usage().
buildFlags=""
if [[ "${GSG_UAT_RACE:-}" == "1" ]]
then
    buildFlags="-race"
    export GORACE="halt_on_error=1"
    echo "race detector ON: any data race aborts the run"
fi
go build $buildFlags
finish

# Verification deliberately goes through gsutil and the aws cli rather than
# through gsg, so that gsg cannot be the thing that validates its own output.
if [[ "$target" == "gs" || "$target" == "all" ]]
then
    requireTool gsutil gs
    do_test gs
fi

if [[ "$target" == "s3" || "$target" == "all" ]]
then
    requireTool aws s3
    do_test s3
fi
