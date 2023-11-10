set -e
testid=`date +%s`
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

assert() {
    if [[ "$2" == "remote" ]]
    then
        if gsutil ls $remote_base/$1 &>/dev/null
        then
            echo OK: $1 exists remotely.
        else
            echo FATAL: required file $1 does not exists remotely.
            exit 1
        fi
    else
        if ls $1 &>/dev/null
        then
            echo OK: $1 exists locally.
        else
            echo FATAL: required file $1 does not exists locally.
            exit 1
        fi
    fi
}

assert_not() {
    if asset $1 $2
    then
        echo FATAL: assert_not failed for file $1
        exit 1
    else
        echo OK: $1 does not exists
    fi
}

remote_copy() {
    local r=""
    case $1 in
    gs)
        if [[ "$2" == "true" ]]
        then
            r=" -r"
        fi
        echo "gsutil cp $r"
        ;;
    s3)
        if [[ "$2" == "true" ]]
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

do_test() {
    mode=$1
    remote_base="$1$remote_base_template"

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
    touch to_upload
    ../gsg cp to_upload $remote_base/to_upload
    assert to_upload remote
    finish
    ftu="folder_to_upload"

    start "test upload a folder"
    mkdir -p $ftu/a/b/c
    touch $ftu/a/1.txt
    touch $ftu/a/2.txt
    touch $ftu/a/b/c/3.txt
    ../gsg cp -r $ftu $remote_base/$ftu
    assert $ftu/a/1.txt remote
    assert $ftu/a/2.txt remote
    assert $ftu/a/b/c/3.txt remote
    finish

    start "test download"

    start "test download single file"
    touch to_download
    $(remote_copy $mode) to_download $remote_base/to_download
    rm to_download
    ../gsg cp $remote_base/to_download to_download
    assert to_download
    finish

    ftd="folder_to_download"
    start "test download a folder"
    mkdir -p $ftd/a/b/c
    touch $ftd/a/1.txt
    touch $ftd/a/2.txt
    touch $ftd/a/b/c/3.txt
    $(remote_copy $mode true) $ftd $remote_base/$ftd
    rm -rf $ftd
    ../gsg cp -r $remote_base/$ftd $ftd
    assert $ftd/a/1.txt
    assert $ftd/a/2.txt
    assert $ftd/a/b/c/3.txt
    finish


    start "testing rsync"
    ftr="folder_to_rsync"

    start "test rsync a local folder to remote"
    mkdir -p $ftr/a/b/c
    touch $ftr/a/1.txt
    touch $ftr/a/2.txt
    touch $ftr/a/b/c/3.txt
    ../gsg rsync -r $ftr $remote_base/$ftr
    assert $ftr/a/1.txt remote
    assert $ftr/a/2.txt remote
    assert $ftr/a/b/c/3.txt remote
    echo "whocares" > $ftr/a/1.txt
    ../gsg rsync -r $ftr $remote_base/$ftr
    $(remote_copy $mode) $remote_base/$ftr/a/1.txt $ftr/a/1_remote.txt
    same $ftr/a/1.txt $ftr/a/1_remote.txt
    finish

    start "test rsync a remote folder to local"
    rm -rf $ftr
    ../gsg rsync -r $remote_base/$ftr $ftr
    assert $ftr/a/1.txt remote
    assert $ftr/a/2.txt remote
    assert $ftr/a/b/c/3.txt remote
    finish

    start "test rsync with -d and non-existing src"
    ../gsg rsync -d whocares $remote_base/$ftr
    assert_not $ftr/a/1.txt remote
    assert_not $ftr/a/2.txt remote
    assert_not $ftr/a/b/c/3.txt remote
    finish


    start "leaving $testbase"
    popd
    finish

    start "cleanup test ground"
    rm -rf uat_temp || true
    case $mode in
    gs)
        gsutil ls  gs://gsg-uat | xargs -I {} gsutil -m rm -r {} || true
        ;;
    s3)
        aws s3 rm s3://gsg-uat --recursive || true
        ;;
    esac
    
    finish

    finish "everything OK with $mode !"
}

start "building gsg binary"
go build
finish

do_test gs
do_test s3
