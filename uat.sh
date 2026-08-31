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
        oci)
            # $remote_base is oci://<bucket>/<testid>, so the object name is
            # everything after the bucket. head is the cheapest existence
            # check; get --file - streams the body to stdout.
            if oci os object head --namespace "$oci_ns" --bucket-name "$oci_bucket" \
                --name "$testid/$1" >/dev/null 2>&1
            then
                content="$(oci os object get --namespace "$oci_ns" --bucket-name "$oci_bucket" \
                    --name "$testid/$1" --file - 2>/dev/null)"
                if [[ "$content" == "$2" ]]
                then
                    echo OK: $1 exists with correct content remotely.
                else
                    echo FATAL: required file $1 does not have correct content remotely.
                    echo "  wanted [$2] got [$content]"
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
        oci)
            # $remote_base is oci://<bucket>/<testid>, so the object name is
            # everything after the bucket. head is the cheapest existence
            # check; get --file - streams the body to stdout.
            if oci os object head --namespace "$oci_ns" --bucket-name "$oci_bucket" \
                --name "$testid/$1" >/dev/null 2>&1
            then
                content="$(oci os object get --namespace "$oci_ns" --bucket-name "$oci_bucket" \
                    --name "$testid/$1" --file - 2>/dev/null)"
                if [[ "$content" == "$testid" ]]
                then
                    echo OK: $1 exists with correct content remotely.
                else
                    echo FATAL: required file $1 does not have correct content remotely.
                    echo "  wanted [$testid] got [$content]"
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
        oci)
            if oci os object head --namespace "$oci_ns" --bucket-name "$oci_bucket" \
                --name "$testid/$1" >/dev/null 2>&1
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

# remote_count reports how many objects the provider itself has under a path.
#
# Deliberately the provider CLI rather than gsg: it is what lets a case prove
# its own fixture landed where it thinks, so an assertion cannot pass because
# the path it names does not exist.
remote_count() {
    case $mode in
    gs)
        gsutil ls -r "$remote_base/$1" 2>/dev/null | grep -v ':$' | grep -c . || true
        ;;
    s3)
        aws s3 ls "s3://gsg-uat/$testid/$1" --recursive 2>/dev/null | wc -l | tr -d ' '
        ;;
    oci)
        oci os object list --namespace "$oci_ns" --bucket-name "$oci_bucket" \
            --prefix "$testid/$1" --all --query 'length(data)' 2>/dev/null || echo 0
        ;;
    esac
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

# newTmpCaches prints the md5-named files that appeared in /tmp since the last
# snapshotTmp. gsg names a lock receipt after a hash of bucket and object, which
# the shell cannot recompute portably, so it is found by diffing instead.
newTmpCaches() {
    ls /tmp > .tmp_after 2>/dev/null || true
    comm -13 .tmp_before .tmp_after 2>/dev/null | grep -E '^[0-9a-f]{32}$' || true
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
    # chmod first: a case that sets a directory to 000 restores it afterwards,
    # but a run killed in between leaves one behind that rm cannot enter, and
    # every later run then fails at setup with "Directory not empty".
    chmod -R u+rwX uat_temp 2>/dev/null || true
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
    # 34, not 27. This used to report 27: batchAttrs short-circuited every key
    # ending in "/" and synthesized an empty GetObjectAttributesOutput for it,
    # so the marker's size was never read and its 7 bytes went uncounted --
    # right for a common prefix, which has no size, and wrong for a real object
    # that happens to end in "/". That was TODO item 11, pinned here so that
    # changing it would be deliberate.
    #
    # Carrying the size through from the listing keeps the two kinds apart, so
    # the marker is now counted, which is what item 11 asked for and what the
    # aws cli itself reports: "aws s3 ls --recursive --summarize" over the same
    # three objects gives 27 where gsg gave 20, and both now say 27.
    assertEq "a marker's bytes are counted (TODO item 11)" \
        "$(../gsg du -s $remote_base/$fdu 2>/dev/null | awk '{print $1}')" "34"
    assertEq "and they land in the sub/ subtotal" \
        "$(../gsg du $remote_base/$fdu 2>/dev/null | grep "/$fdu/sub/$" | awk '{print $1}')" "24"
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
    # Concurrent attribute requests are capped at 64. Every other fixture here
    # is small enough that the cap never engages, so nothing covered the path
    # where work queues behind it.
    #
    # On s3 the fan-out is now the checksum batch rather than one request per
    # listed object, and only a caller that compares checksums triggers it --
    # so ls and du below no longer reach it, and the rsync does.
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
    # It takes two rsyncs to reach the cap. The first copies into an empty
    # directory, where there is nothing to compare against, so no checksum is
    # ever read and the batch never fires. The second compares 70 objects,
    # which is what queues work behind a cap of 64.
    rm -rf ${fmany}_sync && mkdir -p ${fmany}_sync
    ../gsg -m rsync -r $remote_base/$fmany ${fmany}_sync >/dev/null 2>&1
    assertEq "an rsync past the cap brings back every object" \
        "$(find ${fmany}_sync -type f | wc -l | tr -d ' ')" "70"
    assertOk "and the tree matches" diff -r $fmany ${fmany}_sync
    assertEq "a second rsync compares all 70 and copies nothing" \
        "$(../gsg -m rsync -r $remote_base/$fmany ${fmany}_sync 2>&1 | grep -c 'No diff detected')" "1"
    assertOk "and the tree still matches" diff -r $fmany ${fmany}_sync
    rm -rf ${fmany}_sync
    finish

    start "regression: a listing reports the sizes and times the service has"

    # s3 used to answer a listing by taking the keys and throwing away the size
    # and modification time that came with them, then issuing one
    # GetObjectAttributes per key to fetch the same two values back. A million
    # keys meant a million extra requests for data already in hand, and a
    # million independent chances to fail -- and a failed lookup dropped that
    # object from the result. The listing now carries them.
    #
    # What this case does NOT do is prove the extra requests are gone: reverting
    # to eager per-object fetches would satisfy every assertion below, since the
    # values would be identical. Counting requests is not something a shell can
    # do against a live service, so that half is measured -- ls -r over 1006
    # objects went 0.88-1.05s to 0.45s -- and stated here rather than asserted.
    #
    # So what has to hold is that the values did not change in the move. Sizes
    # are asserted directly; the modification time is asserted through rsync,
    # which is what actually depends on it: cp sets the local mtime from the
    # object's, so a second rsync is a no-op only if the time the listing
    # reported matches the one the download applied. A listing that reported a
    # zero time -- what dropping the field would do -- makes that second rsync
    # copy the whole tree again.
    fattr="folder_listattrs"
    mkdir -p $fattr
    printf '1' > $fattr/one.txt
    printf '1234567890' > $fattr/ten.txt
    printf '%.0s1' $(seq 1 300) > $fattr/threehundred.txt
    ../gsg -m cp -r $fattr $remote_base/$fattr >/dev/null 2>&1

    lsize() { ../gsg ls -l -r "$remote_base/$fattr" 2>/dev/null | awk -v n="$1" '$3 ~ n {print $1}'; }
    assertEq "a 1-byte object lists as 1 byte" "$(lsize 'one\.txt$')" "1"
    assertEq "a 10-byte object lists as 10 bytes" "$(lsize 'ten\.txt$')" "10"
    assertEq "a 300-byte object lists as 300 bytes" "$(lsize 'threehundred\.txt$')" "300"
    assertEq "and du -s agrees with the total on disk" \
        "$(../gsg du -s $remote_base/$fattr 2>/dev/null | awk '{print $1}')" \
        "$(cat $fattr/* | wc -c | tr -d ' ')"

    # Every listed time must be a real one. A dropped LastModified prints as
    # the zero time, which is the failure this catches.
    assertEq "no object lists a zero modification time" \
        "$(../gsg ls -l -r $remote_base/$fattr 2>/dev/null | grep -c '0001-01-01')" "0"
    assertEq "and every object lists one" \
        "$(../gsg ls -l -r $remote_base/$fattr 2>/dev/null | grep -cE '[0-9]{4}-[0-9]{2}-[0-9]{2}T')" "3"

    rm -rf ${fattr}_sync && mkdir -p ${fattr}_sync
    ../gsg -m rsync -r $remote_base/$fattr ${fattr}_sync >/dev/null 2>&1
    assertEq "a second rsync copies nothing" \
        "$(../gsg -m rsync -r $remote_base/$fattr ${fattr}_sync 2>&1 | grep -c 'No diff detected')" "1"
    assertOk "and the tree matches" diff -r $fattr ${fattr}_sync
    rm -rf ${fattr}_sync $fattr
    finish

    start "regression: an unknown checksum is a difference, not a match"

    # The checksum a listing does not carry is fetched on demand, and that
    # fetch can come back with nothing: the object may carry no comparable
    # CRC32C at all -- anything uploaded by "aws s3 cp" does not -- or the
    # request may fail. A bare number cannot tell either of those from a
    # genuine checksum of 0, and Attrs.Same compared that 0 as if it were real.
    #
    # Both sides have to fail for it to bite, which rules out the local cases
    # (a local file always has a checksum to compute) but not a cloud-to-cloud
    # rsync, where both sides are objects. With -v the modification time is
    # skipped by design, so path and size are all that is left -- and two
    # different objects of the same size compared equal. rsync then left the
    # stale destination in place and reported no diff.
    #
    # Measured against a build carrying the old behaviour: destination stays
    # BBBB. With the fix it becomes AAAA.
    #
    # s3 only: gs objects always carry a CRC32C from the service, so neither
    # side can come back empty, and rsync between different schemes is refused.
    if [[ "$mode" != "s3" ]]
    then
        echo "SKIP: gs objects always carry a checksum, so neither side can be unknown"
        finish
    else
    funk="folder_unknowncrc"
    mkdir -p $funk
    printf 'AAAA' > $funk/a
    printf 'BBBB' > $funk/b
    # aws s3 cp, deliberately: it stores no comparable CRC32C, which is the
    # condition being tested. Uploading with gsg would store one and the two
    # objects would differ on checksum like any other pair.
    aws s3 cp --quiet $funk/a "s3://gsg-uat/$testid/$funk/src/a.txt" >/dev/null
    aws s3 cp --quiet $funk/b "s3://gsg-uat/$testid/$funk/dst/a.txt" >/dev/null
    assertEq "the two objects really are the same size" \
        "$(aws s3api head-object --bucket gsg-uat --key "$testid/$funk/src/a.txt" --query ContentLength --output text 2>/dev/null)" \
        "$(aws s3api head-object --bucket gsg-uat --key "$testid/$funk/dst/a.txt" --query ContentLength --output text 2>/dev/null)"

    ../gsg rsync -r -v "$remote_base/$funk/src" "$remote_base/$funk/dst" >/dev/null 2>&1
    assertEq "rsync -v replaces an object it cannot compare" \
        "$(aws s3 cp "s3://gsg-uat/$testid/$funk/dst/a.txt" - 2>/dev/null)" "AAAA"

    rm -rf $funk
    finish
    fi

    start "regression: a bucket in another region"
    # One S3 client was cached for the whole process, with its region taken
    # from whichever bucket was touched first, so a later request for a bucket
    # elsewhere went to the wrong endpoint and came back 301 MovedPermanently.
    # S3Attrs then reported that as "not an object", so the failure looked like
    # an absence. Verified by hand against a bucket in us-west-2 while the
    # client was pinned to ap-southeast-1: an object that exists reads as
    # ok=false on main and ok=true with the fix.
    #
    # Exercising it here needs a second WRITABLE bucket in a DIFFERENT region,
    # which this harness will not create on someone's account. Name one in
    # GSG_UAT_BUCKET2 to run it. The classification half -- which errors mean
    # "absent" and which must be reported -- is covered by TestIsNotFound.
    if [[ "$mode" != "s3" || -z "${GSG_UAT_BUCKET2:-}" ]]
    then
        if [[ "$mode" == "s3" ]]
        then
            echo "SKIP: set GSG_UAT_BUCKET2 to a writable bucket in another region to run this"
        else
            echo "SKIP: gs has no per-region client to get wrong"
        fi
        finish
    else
    freg="folder_region"
    mkdir -p $freg
    prepare_file $freg/a.txt crossregion
    # Touch gsg-uat first, then the other bucket, in one process. Before the
    # fix the second used the first's region.
    ../gsg -m cp -r $freg $remote_base/$freg
    assertOk "a cross-region copy in one process succeeds" \
        ../gsg cp $remote_base/$freg/a.txt "s3://${GSG_UAT_BUCKET2}/$testid/a.txt"
    assertEq "and the object really landed in the other bucket" \
        "$(aws s3 cp "s3://${GSG_UAT_BUCKET2}/$testid/a.txt" - 2>/dev/null)" "crossregion"
    aws s3 rm "s3://${GSG_UAT_BUCKET2}/$testid/a.txt" >/dev/null 2>&1
    finish
    fi

    start "regression: -v verifies, and a repeated rsync is a no-op"
    # Two faults that shared a cause. S3 reports a checksum as base64 of the
    # raw bytes, and it was parsed with ParseUint base 10, so every object's
    # checksum read back as 0. Nothing local ever matched, so an rsync from s3
    # re-downloaded the whole tree on every run. And -v never verified at all:
    # forceChecksum only set ChecksumMode on the request, and the function that
    # compares was defined and called from nowhere.
    fver="folder_verify"
    mkdir -p $fver
    prepare_file $fver/a.txt verify_a
    prepare_file $fver/b.txt verify_b
    ../gsg -m cp -r $fver $remote_base/$fver
    rm -rf ${fver}_down && mkdir -p ${fver}_down
    assertEq "cp -v actually checks the downloaded files" \
        "$(../gsg -m cp -r -v $remote_base/$fver ${fver}_down 2>&1 | grep -c 'CRC32C checking success')" "2"

    rm -rf ${fver}_sync && mkdir -p ${fver}_sync
    ../gsg -m rsync -r $remote_base/$fver ${fver}_sync >/dev/null 2>&1
    assertEq "a second rsync copies nothing" \
        "$(../gsg -m rsync -r $remote_base/$fver ${fver}_sync 2>&1 | grep -c 'No diff detected')" "1"
    assertOk "and the tree still matches" diff -r $fver ${fver}_sync
    finish

    start "regression: a listing that spans more than one page"
    # ListObjectsV2 returns at most 1000 keys per page. The loop used to stop
    # when a page carried no Contents, which is wrong for a delimited listing:
    # a page whose keys all collapsed into common prefixes has none, so a
    # prefix with more than a page of subdirectories stopped after the first
    # and the rest went missing with no error. Measured on main: 1000 of 1005.
    #
    # s3 only. The gs backend hands pagination to the client's iterator and
    # already returns all of them; uploading a second 1005 object fixture to
    # prove that costs minutes and shows nothing new.
    if [[ "$mode" != "s3" ]]
    then
        echo "SKIP: gs paginates through its client iterator and was never affected"
        finish
    else
    fpage="folder_paged"
    mkdir -p $fpage
    i=1
    while [[ $i -le 1005 ]]
    do
        d=$fpage/$(printf 'd%04d' $i)
        mkdir -p "$d" && printf 'x' > "$d/f.txt"
        i=$((i + 1))
    done
    # Uploaded with the provider CLI rather than gsg: it parallelises well, and
    # the point of the case is what gsg reads back, not how it got there.
    aws s3 cp --recursive --quiet $fpage "$remote_base/$fpage/" >/dev/null 2>&1
    assertEq "the provider really has 1005 subdirectories" \
        "$(aws s3 ls "$remote_base/$fpage/" | grep -c PRE)" "1005"
    assertEq "ls without -r returns every one of them" \
        "$(../gsg ls $remote_base/$fpage 2>/dev/null | wc -l | tr -d ' ')" "1005"
    assertEq "ls -r returns every object" \
        "$(../gsg ls -r $remote_base/$fpage 2>/dev/null | wc -l | tr -d ' ')" "1005"
    assertEq "du -s totals all of them" \
        "$(../gsg du -s $remote_base/$fpage 2>/dev/null | awk '{print $1}')" "1005"
    aws s3 rm "$remote_base/$fpage" --recursive --quiet >/dev/null 2>&1
    rm -rf $fpage
    finish
    fi

    start "regression: cat returns exact content"
    assertEq "cat direct.txt" "$(../gsg cat $remote_base/$fdu/direct.txt 2>/dev/null)" "0123456789"
    finish

    start "regression: a truncated crc32c cache must not crash the next run"
    # The cache write was not atomic: the file was created, then the four bytes
    # were written. A run killed in between left an empty file in /tmp for good,
    # and every later run decoded it as a uint32 and panicked with
    # "index out of range [3] with length 0".
    #
    # Runs on both backends now. It used to be gs only, because S3.Download
    # took forceChecksum, used it solely to set ChecksumMode on the GetObject
    # request, and never called MustEqualCRC32C -- so -v computed no checksum
    # on the s3 path and left no cache for this case to corrupt.
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
    # B's lock is taken by gsg itself, not written out of band. That matters:
    # an earlier version of this case substituted an object with different
    # content, and passed even while the bug was live, because every gsg lock
    # used to hold the same byte and therefore the same ETag -- so s3's
    # If-Match matched anybody's lock. Only two real locks exercise that.
    aunl="unlock_other.lock"
    snapshotTmp
    ../gsg lock $remote_base/$aunl 3600            # A locks
    receipt=""
    for f in $(newTmpCaches); do receipt="/tmp/$f"; done
    if [[ -z "$receipt" ]]
    then
        echo "FATAL: lock left no receipt in /tmp to work with"
        exit 1
    fi
    cp "$receipt" .A.receipt

    # A's lock goes away and B takes a fresh one at the same path.
    case $mode in
    gs) gsutil rm "$remote_base/$aunl" >/dev/null 2>&1 ;;
    s3) aws s3api delete-object --bucket gsg-uat --key "$testid/$aunl" >/dev/null 2>&1 ;;
    esac
    ../gsg lock $remote_base/$aunl 3600            # B locks, overwriting the receipt
    cp .A.receipt "$receipt"                       # A still holds its own, now stale

    if ../gsg unlock $remote_base/$aunl >/dev/null 2>&1
    then
        echo "FATAL: unlock reported success on a lock held by someone else"
        exit 1
    fi
    echo "OK: unlock refuses a lock it does not hold"
    if ! ../gsg cat $remote_base/$aunl >/dev/null 2>&1
    then
        echo "FATAL: the other holder's lock was deleted"
        exit 1
    fi
    echo "OK: the other holder's lock survives"
    rm -f .A.receipt "$receipt"
    case $mode in
    gs) gsutil rm "$remote_base/$aunl" >/dev/null 2>&1 ;;
    s3) aws s3api delete-object --bucket gsg-uat --key "$testid/$aunl" >/dev/null 2>&1 ;;
    esac
    finish

    start "regression: only one of several contenders may take a lock"
    # gsg lock existed for mutual exclusion but s3 never provided any: the
    # acquire path headed the object, deleted it unconditionally if expired,
    # then put unconditionally, so every contender came away believing it held
    # the lock. Measured on main, eight racing processes: seven acquired.
    # A fresh key per run, because gs rate limits writes to one object name.
    aexcl="contended-$$.lock"
    rm -f .winners; touch .winners
    for i in 1 2 3 4 5 6 7 8
    do
        ( ../gsg lock $remote_base/$aexcl 300 >/dev/null 2>&1; echo $? >> .winners ) &
    done
    wait
    assertEq "exactly one of eight contenders acquires the lock" \
        "$(grep -c '^0$' .winners)" "1"
    rm -f .winners
    case $mode in
    gs) gsutil rm "$remote_base/$aexcl" >/dev/null 2>&1 ;;
    s3) aws s3api delete-object --bucket gsg-uat --key "$testid/$aexcl" >/dev/null 2>&1 ;;
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

    start "regression: an upload is checked on arrival, not after it"
    # gs stores a CRC32C for every object whether asked or not -- but it
    # computes it from whatever reached it. Nothing was transmitted, so a body
    # corrupted in transit was stored together with a checksum of the corrupted
    # bytes: rsync then saw a matching object and -v verified the corruption
    # against itself and passed.
    #
    # gs only. s3 has been checked since #47 and oci since #52.
    if [[ "$mode" != "gs" ]]
    then
        echo "SKIP: only the gs upload path was missing this"
        finish
    else
    fint="folder_integrity"
    mkdir -p $fint
    prepare_file $fint/a.txt
    ../gsg cp $fint/a.txt $remote_base/$fint/a.txt
    assert $fint/a.txt remote

    # Ordinary work must not pay for the check. A file that changes and is
    # uploaded again has to succeed on the first attempt, storing the new
    # bytes -- not fail, recompute and retry.
    prepare_file $fint/edited.txt first
    ../gsg cp $fint/edited.txt $remote_base/$fint/edited.txt
    assertValue $fint/edited.txt first remote

    prepare_file $fint/edited.txt second
    editout=$(../gsg cp $fint/edited.txt $remote_base/$fint/edited.txt 2>&1) && editrc=0 || editrc=$?
    assertEq "re-uploading a changed file succeeds" "$editrc" "0"
    assertEq "and it was not refused and retried" \
        "$(echo "$editout" | grep -c "doesn't match calculated CRC32C")" "0"
    assertValue $fint/edited.txt second remote

    # The same, with the modification time put back exactly. The checksum is
    # taken from the handle the body is read from rather than from the cache
    # keyed on path and mtime, so this is just another upload -- when it came
    # from the cache it was a whole upload spent to be told the checksum was
    # stale.
    prepare_file $fint/sneaky.txt before
    ../gsg cp $fint/sneaky.txt $remote_base/$fint/sneaky.txt

    # Rewritten by a helper rather than by touch, because touch works to the
    # second while the cache key carries nanoseconds -- a touched file lands on
    # a different key and is recomputed, so it would pass whatever the code
    # did. Putting the modification time back exactly is what makes the cache
    # look valid when it is not.
    cat > sneaky_helper.go <<'GOHELPER'
package main

import (
	"os"
	"time"
)

func main() {
	p, content := os.Args[1], os.Args[2]
	fi, err := os.Stat(p)
	if err != nil {
		os.Exit(1)
	}
	mt := fi.ModTime()
	if err = os.WriteFile(p, []byte(content+"\n"), 0600); err != nil {
		os.Exit(1)
	}
	if err = os.Chtimes(p, time.Now(), mt); err != nil {
		os.Exit(1)
	}
}
GOHELPER
    (cd .. && go run "$OLDPWD/sneaky_helper.go" "$OLDPWD/$fint/sneaky.txt" afterx)
    rm -f sneaky_helper.go
    sneakout=$(../gsg cp $fint/sneaky.txt $remote_base/$fint/sneaky.txt 2>&1) && sneakrc=0 || sneakrc=$?
    assertEq "a change that keeps the modification time still uploads" "$sneakrc" "0"
    assertEq "and is not refused either" \
        "$(echo "$sneakout" | grep -c "doesn't match calculated CRC32C")" "0"
    assertValue $fint/sneaky.txt afterx remote

    # And the checksum really is transmitted. Built with an overlay so the
    # source tree is untouched: a gsg that sends a checksum one off from the
    # body must have the object refused. Without the checksum being sent at
    # all, this upload would simply succeed.
    ovdir=$(mktemp -d)
    sed 's/return h.Sum32(), nil/return h.Sum32() + 1, nil/' ../gcs/gcs.go > "$ovdir/gcs.go"
    printf '{"Replace":{"%s/gcs/gcs.go":"%s/gcs.go"}}' "$repoRoot" "$ovdir" > "$ovdir/overlay.json"
    (cd .. && go build -overlay "$ovdir/overlay.json" -o "$ovdir/gsg_wrongcrc" .) >/dev/null 2>&1

    if "$ovdir/gsg_wrongcrc" cp $fint/a.txt "$remote_base/$fint/wrongcrc.txt" >/dev/null 2>&1
    then
        echo "FATAL: an upload carrying a checksum that does not match its body was accepted"
        rm -rf "$ovdir"
        exit 1
    else
        echo "OK: an upload whose checksum does not match its body is refused"
    fi
    assert_not $fint/wrongcrc.txt remote
    rm -rf "$ovdir"
    finish
    fi

    start "regression: an upload the server rejects must not report success"
    # GCS finalizes an upload in Close, and that is where the server's answer
    # arrives -- io.Copy only fills the writer's buffer. Upload deferred that
    # Close and discarded its error, so any commit-time failure was reported as
    # a success: a rejected object name, a quota, a permission failure at
    # finalize, a dropped connection. cp exited 0 having stored nothing, and
    # rsync's retry never fired because there was no error to retry on.
    #
    # gs only. The trigger used here is an object name containing a newline,
    # which GCS rejects with "Disallowed unicode characters present in object
    # name"; s3 accepts such keys, and its Upload checks PutObject's error
    # anyway, so it never had this defect.
    if [[ "$mode" != "gs" ]]
    then
        echo "SKIP: s3 accepts these names and already checks its upload error"
        finish
    else
    frej="folder_rejected"
    nl2=$'\n'
    mkdir -p $frej
    prepare_file $frej/fine.txt accepted
    printf 'x' > "$frej/we${nl2}ird.txt"
    if ../gsg -m cp -r $frej $remote_base/$frej >/dev/null 2>&1
    then
        echo "FATAL: cp reported success although the server rejected an object"
        exit 1
    fi
    echo "OK: cp fails when the server rejects an object at commit"
    # The one it could store is still stored, and the rejected one is absent.
    assertValue $frej/fine.txt accepted remote
    assert_not "$frej/we${nl2}ird.txt" remote
    finish
    fi

    start "regression: mv must not delete what it just moved"
    # mv is a copy followed by a delete of the source. The delete used to list
    # the source *after* the copy, so when the destination lived inside the
    # source that listing returned the fresh copies too and deleting them threw
    # away the data the move had produced. Measured on gs: two objects to none.
    #
    # And a move onto the same path is refused outright, as gsutil refuses it,
    # because there the copy produces nothing for the delete to spare.
    fmv="folder_mv"
    mkdir -p $fmv/sub
    prepare_file $fmv/a.txt
    prepare_file $fmv/sub/b.txt
    ../gsg -m cp -r $fmv $remote_base/$fmv

    # Onto itself: refused, and everything still there. A trailing slash names
    # the same place and must be refused too -- it is one keystroke away.
    for spelling in "$remote_base/$fmv" "$remote_base/$fmv/"
    do
        if ../gsg -m mv -r "$remote_base/$fmv" "$spelling" >/dev/null 2>&1
        then
            echo "FATAL: moving $fmv onto $spelling was allowed"
            exit 1
        fi
    done
    echo "OK: a move onto the same path is refused, with or without a trailing slash"
    assertEq "and nothing was removed" \
        "$(../gsg ls -r $remote_base/$fmv 2>/dev/null | wc -l | tr -d ' ')" "2"

    # Into a directory beneath itself: also refused, and this is the case with
    # teeth. gsg's cp -r copies a directory's contents rather than nesting the
    # directory, so source and destination keys collide: with a.txt at both
    # $fmv and $fmv/sub, `mv -r $fmv $fmv/sub` would write $fmv/a.txt over
    # $fmv/sub/a.txt and then delete the latter as a source. Measured before
    # the guard, that left one object where there had been two, and the
    # surviving one held the wrong contents.
    #
    # gsutil allows the same command because it nests -- it ends at
    # $fmv/sub/$fmv/... where nothing collides. gsg cannot borrow that without
    # changing what cp -r means.
    prepare_file $fmv/collide.txt outer
    prepare_file $fmv/sub/collide.txt inner
    ../gsg cp $fmv/collide.txt $remote_base/$fmv/collide.txt
    ../gsg cp $fmv/sub/collide.txt $remote_base/$fmv/sub/collide.txt

    if ../gsg -m mv -r "$remote_base/$fmv" "$remote_base/$fmv/sub" >/dev/null 2>&1
    then
        echo "FATAL: a move into a subdirectory of the source was allowed"
        exit 1
    else
        echo "OK: a move into a subdirectory of the source is refused"
    fi
    # Both colliding objects still there, each with its own contents.
    assertValue $fmv/collide.txt outer remote
    assertValue $fmv/sub/collide.txt inner remote

    # The guard has to stay out of the way of real moves, or it trades one
    # kind of lost work for another. These are the near misses: a sibling
    # whose name begins with the source's, and a longer name at the same
    # level. Both were refused by an earlier version of the guard that
    # compared prefixes without a slash boundary.
    prepare_file $fmv/near.txt near
    ../gsg cp $fmv/near.txt $remote_base/$fmv/near.txt
    assertOk "a longer name at the same level is a real move" \
        ../gsg mv "$remote_base/$fmv/near.txt" "$remote_base/$fmv/near.txt.bak"
    assertValue $fmv/near.txt.bak near remote
    assert_not $fmv/near.txt remote

    ../gsg -m cp -r $fmv/sub $remote_base/$fmv/d
    assertOk "a sibling whose name starts with the source's is allowed" \
        ../gsg -m mv -r "$remote_base/$fmv/d" "$remote_base/$fmv/dsub"
    assertEq "a sibling whose name starts with the source's is a real move" \
        "$(../gsg ls -r $remote_base/$fmv/dsub 2>/dev/null | wc -l | tr -d ' ')" "2"
    # By exact path: counting ls output would count its "No objects found"
    # line as a result, so an empty prefix reads as one entry.
    assert_not $fmv/d/b.txt remote
    assert_not $fmv/d/collide.txt remote

    # A single object, without -r. That is the other branch of mv entirely --
    # it deletes src.Prefix directly rather than walking a listing -- and
    # nothing exercised it.
    prepare_file $fmv/single.txt lonely
    ../gsg cp $fmv/single.txt $remote_base/$fmv/single.txt
    assertOk "a single object moves without -r" \
        ../gsg mv "$remote_base/$fmv/single.txt" "$remote_base/$fmv/moved_single.txt"
    assertValue $fmv/moved_single.txt lonely remote
    assert_not $fmv/single.txt remote

    finish

    start "regression: what counts as a directory"
    # FileType asks IsDirectory before nearly every command, so what it costs
    # lands on cp, rm, du, mv and rsync alike. It used to answer by listing --
    # recursively on s3, so a prefix holding a million keys walked all million
    # to return a boolean. It now asks the service for the first entry or two.
    #
    # What this case pins is the answer, not the cost: the cost is measured
    # (1005 objects: s3 237ms -> 30ms, gs 198ms -> 103ms, and flat rather than
    # growing) but nothing here would fail if it regressed to listing, because
    # a wall-clock assertion over the network is a flaky one. The size past a
    # page is here so a regression shows up as a visibly slower run.
    #
    # oci has the same case in uat/oci, so it is skipped here.
    if [[ "$mode" == "oci" ]]
    then
        echo "SKIP: uat/oci/30-cat.sh covers this for oci"
        finish
    else
    fbig="folder_isdir"
    mkdir -p $fbig
    i=1
    while [[ $i -le 1005 ]]
    do
        printf 'x' > "$fbig/f$(printf '%04d' $i).txt"
        i=$((i + 1))
    done
    # Uploaded with gsg, not the provider CLI: "aws s3 cp --recursive src dst/"
    # copies the *contents* of src while "gsutil cp -r src dst/" copies the
    # *directory*, so the two clouds ended up with layouts one level apart.
    # Every assertion below that expects "no" then passed on gs for the wrong
    # reason -- the path did not exist at all. Verified against the provider
    # CLI just below, so a fixture that lands in the wrong place fails here
    # rather than quietly weakening the case.
    ../gsg -m cp -r $fbig $remote_base/$fbig >/dev/null 2>&1
    assertEq "the fixture really has 1005 objects directly under it" \
        "$(remote_count $fbig)" "1005"
    assertEq "and f0001.txt really is one of them" \
        "$(remote_count $fbig/f0001.txt)" "1"

    # cp without -r refuses a directory with a distinctive message, which is
    # how IsDirectory's answer becomes observable.
    isdir() {
        local out
        out=$(../gsg cp "$remote_base/$1" ./sink_isdir 2>&1) || true
        echo "$out" | grep -q "Did you mean" && echo yes || echo no
    }
    assertEq "a directory of more than one page is a directory" "$(isdir $fbig)" "yes"
    assertEq "the same directory named with a trailing slash too" "$(isdir $fbig/)" "yes"
    assertEq "one of its objects is not" "$(isdir $fbig/f0001.txt)" "no"
    assertEq "nor is a partial name inside it" "$(isdir $fbig/f000)" "no"
    assertEq "nor is something absent" "$(isdir $fbig/nothing-here)" "no"

    # A directory whose children are all sub-directories carries no keys in a
    # delimited listing, only common prefixes -- the shape that truncated a
    # listing in item 2, and the one a single-entry request could easily miss.
    fsubs="${fbig}_subs"
    mkdir -p $fsubs/only/deeper
    prepare_file $fsubs/only/deeper/x.txt
    ../gsg -m cp -r $fsubs $remote_base/$fsubs >/dev/null 2>&1
    assertEq "the sub-directory fixture landed where expected" \
        "$(remote_count $fsubs/only/deeper/x.txt)" "1"
    assertEq "and nothing sits directly under it" \
        "$(remote_count $fsubs/only/deeper/x.txt)" "$(remote_count $fsubs)"
    assertEq "a directory holding only sub-directories is a directory" \
        "$(isdir $fsubs)" "yes"
    assertEq "a nested directory is a directory" "$(isdir $fsubs/only)" "yes"
    # The partial-name trap on the CommonPrefixes side rather than the
    # Contents side: "on" is a string prefix of the sub-directory "only/", so
    # without the trailing slash the service would match it.
    assertEq "a partial directory name is not a directory" "$(isdir $fsubs/on)" "no"

    # An object whose name is exactly the prefix -- the zero-byte marker the
    # console's "create folder" writes -- is the prefix, not something beneath
    # it. gsutil hands it back as an object, so gsg does too. It sorts before
    # everything under it, which is the trap: asking for a single entry would
    # return only the marker and make a directory carrying one look empty.
    fmk="folder_marker"
    case $mode in
    s3)
        : > empty_marker
        aws s3api put-object --bucket gsg-uat --key "$testid/$fmk/lone/" \
            --body empty_marker >/dev/null
        aws s3api put-object --bucket gsg-uat --key "$testid/$fmk/both/" \
            --body empty_marker >/dev/null
        rm -f empty_marker
        prepare_file marker_child.txt
        $(remote_copy false) marker_child.txt "$remote_base/$fmk/both/child.txt" >/dev/null 2>&1
        rm -f marker_child.txt
        ;;
    gs)
        # gsutil cannot write a name ending in a slash -- it appends the local
        # basename -- so this goes through the storage client directly.
        cat > marker_helper.go <<'GOHELPER'
package main

import (
	"context"
	"os"

	"cloud.google.com/go/storage"
)

func main() {
	ctx := context.Background()
	c, err := storage.NewClient(ctx)
	if err != nil {
		os.Exit(1)
	}
	for _, name := range os.Args[2:] {
		w := c.Bucket(os.Args[1]).Object(name).NewWriter(ctx)
		if name[len(name)-1] != '/' {
			_, _ = w.Write([]byte("child\n"))
		}
		if err = w.Close(); err != nil {
			os.Exit(1)
		}
	}
}
GOHELPER
        (cd .. && go run "$OLDPWD/marker_helper.go" gsg-uat \
            "$testid/$fmk/lone/" "$testid/$fmk/both/" "$testid/$fmk/both/child.txt")
        rm -f marker_helper.go
        ;;
    esac
    assertEq "a marker with nothing beneath it is an object, not a directory" \
        "$(isdir $fmk/lone/)" "no"
    assertEq "a marker with something beneath it is still a directory" \
        "$(isdir $fmk/both/)" "yes"
    # Without the trailing slash the marker is something *beneath* the path
    # asked about, so the answer flips. gsutil draws the line the same way,
    # and so did the listing this replaced.
    assertEq "the same marker named without a trailing slash is a directory" \
        "$(isdir $fmk/lone)" "yes"

    rm -rf $fbig $fsubs sink_isdir
    finish
    fi

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

    # A command that fails must say so. common.Exit is a bare os.Exit(1), and
    # the command layer used to discard the error it was handed, so a failure
    # the backend had not already logged left an empty screen and a 1.
    # Measured before the fix: du and cp -r over this same directory both
    # exited 1 printing nothing at all.
    #
    # And the message has to name the reason, not just report a status. "exit
    # status 1" is what exec gives back; the tool wrote the real cause to
    # stderr, which is what should reach the user.
    chmod 000 $flocal/denied
    # `x=$(cmd)` takes cmd's exit status, and the suite runs under `set -e`, so
    # capturing a command that is meant to fail kills the run with no message
    # at all. `&& rc=0 || rc=$?` keeps both the output and the status.
    # rsync announces itself before doing any work, so this is the case that
    # catches a fix which merely tracks "was anything printed": the banner
    # would suppress the error that follows, which is the original bug in the
    # most common path.
    rsout=$(../gsg -m rsync -r $flocal ${flocal}_silentsync 2>&1) && rsrc=0 || rsrc=$?
    rm -rf ${flocal}_silentsync

    duout=$(../gsg du $flocal 2>&1) && durc=0 || durc=$?
    cpout=$(../gsg cp -r $flocal ${flocal}_silent 2>&1) && cprc=0 || cprc=$?
    lsout=$(../gsg ls -r $flocal 2>&1) && lsrc=0 || lsrc=$?
    chmod 755 $flocal/denied
    rm -rf ${flocal}_silent

    # The exit status is asserted as well as the text. Code that printed a
    # reason and then exited 0 would satisfy the message checks alone, and a
    # command that fails has to fail.
    assertEq "du of an unreadable tree fails" "$durc" "1"
    assertEq "cp -r of an unreadable tree fails" "$cprc" "1"
    assertEq "ls -r of an unreadable tree fails" "$lsrc" "1"

    # And each has to name the reason the tool gave, not a bare status. "exit
    # status 1" is what exec hands back; find, du and cp each wrote the real
    # cause to stderr.
    assertEq "rsync of an unreadable tree fails" "$rsrc" "1"
    assertEq "and explains itself despite having printed a banner first" \
        "$([ "$(echo "$rsout" | grep -c 'Permission denied')" -ge 1 ] && echo yes || echo no)" "yes"

    for pair in "du|$duout" "cp|$cpout" "ls|$lsout"
    do
        what="${pair%%|*}"; text="${pair#*|}"
        # At least once, not exactly once: cp's own stderr reaches the
        # terminal alongside gsg's message, so the reason legitimately appears
        # twice there.
        assertEq "$what says why it failed" \
            "$([ "$(echo "$text" | grep -c 'Permission denied')" -ge 1 ] && echo yes || echo no)" "yes"
        assertEq "$what does not merely report an exit status" \
            "$(echo "$text" | grep -c 'exit status')" "0"
    done

    # And not twice. The cloud backends describe the failure before returning
    # it, so the command layer must not repeat what has already been said. A
    # missing bucket is the reliable way to provoke that: gs logs "get objects
    # attributes failed with ..." and then returns the same error.
    # gs only, because the wording is the backend's own.
    if [[ "$mode" == "gs" ]]
    then
        dupout=$(../gsg ls "gs://no-such-bucket-$testid/" 2>&1) || true
        assertEq "a failure the backend already explained is not repeated" \
            "$(echo "$dupout" | grep -c "doesn't exist")" "1"
    fi

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
    chmod -R u+rwX uat_temp 2>/dev/null || true
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

# do_test_oci runs the OCI cases.
#
# It is deliberately separate from do_test rather than another branch inside
# it. The OCI backend is being built one operation per pull request, and if
# every one of those appended to the shared body they would all conflict at the
# same anchor -- which is exactly what happened while landing #45, #46 and #47.
# Instead each case lives in its own file under uat/oci, so a pull request adds
# a file and touches nothing another one touches.
#
# Files are sourced in name order, so the NN- prefix decides sequencing when a
# later case depends on an earlier one. They run inside $testbase with
# $remote_base already set, and may use every helper defined above.
do_test_oci() {
    mode=oci
    oci_bucket="${GSG_UAT_OCI_BUCKET:-nb-oci-sin-test}"
    remote_base="oci://$oci_bucket/$testid"

    # A run that cannot reach OCI has not tested OCI. Saying "everything OK"
    # here would be a false green on the one target whose whole purpose is to
    # exercise this backend, so an explicit `uat.sh oci` fails instead.
    if ! oci_ns=$(oci os ns get --query data --raw-output 2>/dev/null) || [[ -z "$oci_ns" ]]
    then
        if [[ "$target" == "oci" ]]
        then
            echo "FATAL: oci mode needs a working ~/.oci/config -- could not resolve the namespace"
            exit 1
        fi
        echo "skipping oci: could not resolve the namespace from ~/.oci/config"
        return 0
    fi

    trap 'code=$?; if [[ $code -ne 0 ]]; then
        echo
        echo "test data left behind for inspection at: $remote_base"
        echo "remove it with: oci os object bulk-delete --namespace '"$oci_ns"' \\"
        echo "                  --bucket-name '"$oci_bucket"' --prefix '"$testid"' --force"
    fi' EXIT

    start "prepare test ground for mode: oci (namespace $oci_ns, bucket $oci_bucket)"
    rm -rf uat_temp || true
    testbase="uat_temp"
    mkdir $testbase
    finish

    start "entering $testbase"
    pushd $testbase
    finish

    ran=0
    for caseFile in "$repoRoot"/uat/oci/*.sh
    do
        [[ -e "$caseFile" ]] || continue
        # shellcheck source=/dev/null
        source "$caseFile"
        ran=$((ran + 1))
    done
    if [[ $ran -eq 0 ]]
    then
        # Same reasoning: an explicit oci run that executed no case at all must
        # not print "everything OK". While the backend is a skeleton that is
        # the expected state, so it is reported rather than treated as failure,
        # but the wording must never suggest anything was verified.
        echo "note: no case files in uat/oci yet -- nothing was verified"
    fi

    start "leaving $testbase"
    popd
    finish

    start "cleanup test ground"
    rm -rf uat_temp || true
    cleaned=true
    oci os object bulk-delete --namespace "$oci_ns" --bucket-name "$oci_bucket" \
        --prefix "$testid" --force >/dev/null 2>&1 || cleaned=false
    if [[ "$cleaned" != "true" ]]
    then
        echo "WARNING: could not remove $remote_base -- objects may still be there"
    fi
    finish

    trap - EXIT
    if [[ $ran -eq 0 ]]
    then
        finish "oci harness ran, but there are no cases yet -- nothing verified"
    else
        finish "everything OK with oci ($ran case file(s)) !"
    fi
}

usage() {
    echo "usage: $0 [gs|s3|oci|all]    (default: all)"
    echo
    echo "  gs   requires gsutil and GOOGLE_APPLICATION_CREDENTIALS"
    echo "  s3   requires the aws cli and its credentials"
    echo "  oci  requires the oci cli and ~/.oci/config"
    echo
    echo "OCI cases live one per file in uat/oci and are sourced in name"
    echo "order. Override the bucket with GSG_UAT_OCI_BUCKET."
    echo
    echo "Objects are written under <scheme>://gsg-uat/<timestamp> and that"
    echo "prefix is removed at the end."
    echo
    echo "GSG_UAT_RACE=1 builds gsg with the race detector and aborts on any"
    echo "data race. The bulk operations below run with -m, so this genuinely"
    echo "exercises the worker pool. It passes as of the fix for TODO.md item"
    echo "12, and roughly doubles the runtime, so it is worth turning on when"
    echo "touching anything concurrent."
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
gs|s3|oci|all) ;;
*) usage ;;
esac

# Case files under uat/oci are sourced by absolute path: do_test_oci runs from
# inside $testbase, so a relative path would not resolve.
repoRoot="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

start "building gsg binary"
# GSG_UAT_RACE=1 builds with the race detector and makes any data race abort
# the run. Off by default only because it roughly doubles the runtime; it does
# pass. See the note in usage().
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

if [[ "$target" == "oci" || "$target" == "all" ]]
then
    # Only a run that explicitly asked for oci may demand the cli. Adding it to
    # the "all" default would break every machine that runs the suite for gs
    # and s3 today and has no reason to have the oci cli installed.
    if ! command -v oci &>/dev/null
    then
        if [[ "$target" == "oci" ]]
        then
            echo "FATAL: oci is required for mode oci but is not installed"
            exit 1
        fi
        echo "skipping oci: the oci cli is not installed"
    else
        do_test_oci
    fi
fi
