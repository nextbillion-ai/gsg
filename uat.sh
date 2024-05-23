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

assertValue() {
    if [[ "$3" == "remote" ]]
    then
        case $mode in
        gs)
            if gsutil ls $remote_base/$1 &>/dev/null
            then
                content="$(gsutil cat $remote_base/$1 2>/dev/null)"
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
            if aws s3 cp $remote_base/$1 .temp &>/dev/null 
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
        if ls $1 &>/dev/null 
        then
            content=$(cat $1)
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
            if gsutil ls $remote_base/$1 &>/dev/null
            then
                content="$(gsutil cat $remote_base/$1 2>/dev/null)"
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
            if aws s3 cp $remote_base/$1 .temp &>/dev/null 
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
        if ls $1 &>/dev/null 
        then
            content=$(cat $1)
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
            if gsutil ls $remote_base/$1 &>/dev/null
            then
                echo FATAL: required file $1 does exists remotely.
                exit 1
            else
                echo OK: required file $1 does not exists remotely.
            fi
            ;;
        s3)
            if aws s3 cp $remote_base/$1 .temp &>/dev/null 
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
        if ls $1 &>/dev/null 
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
    echo "$value" > $1
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
    ../gsg cp -r $ftu $remote_base/$ftu
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
    ../gsg cp -r $remote_base/$ftd $ftd
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
    ../gsg mv -r $remote_base/$ftm $ftm
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
    ../gsg rsync -r $ftr $remote_base/$ftr
    assert $ftr/a/1.txt remote
    assert $ftr/a/2.txt remote
    assert $ftr/a/b/c/3.txt remote
    echo "whocares" > $ftr/a/1.txt
    ../gsg rsync -r $ftr $remote_base/$ftr
    $(remote_copy) $remote_base/$ftr/a/1.txt $ftr/a/1_remote.txt
    same $ftr/a/1.txt $ftr/a/1_remote.txt
    finish

    start "test rsync a remote folder to local"
    prepare_file $ftr/a/1.txt
    $(remote_copy) $ftr/a/1.txt $remote_base/$ftr/a/1.txt
    rm -rf $ftr
    ../gsg rsync -r $remote_base/$ftr $ftr
    assert $ftr/a/1.txt
    assert $ftr/a/2.txt
    assert $ftr/a/b/c/3.txt
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
