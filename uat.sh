set -e
testid=`date +%s`
remote_base="gs://gsg-uat/$testid"
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

start "building gsg binary"
go build
finish

start "prepare test ground"
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
../gsg cp to_upload gs://gsg-uat/$testid/to_upload
assert to_upload remote
finish
ftu="folder_to_upload"

start "test upload a folder"
mkdir -p $ftu/a/b/c
touch $ftu/a/1.txt
touch $ftu/a/2.txt
touch $ftu/a/b/c/3.txt
../gsg cp -r $ftu gs://gsg-uat/$testid/$ftu
assert $ftu/a/1.txt remote
assert $ftu/a/2.txt remote
assert $ftu/a/b/c/3.txt remote
finish

start "test rsync a local folder to remote"
echo "whocares" > $ftu/a/1.txt
../gsg rsync -r $ftu gs://gsg-uat/$testid/$ftu
gsutil cp gs://gsg-uat/$testid/$ftu/a/1.txt $ftu/a/1_remote.txt
same $ftu/a/1.txt $ftu/a/1_remote.txt
finish

start "test download"

start "test download single file"
touch to_download
gsutil cp to_download gs://gsg-uat/$testid/to_download
rm to_download
../gsg cp gs://gsg-uat/$testid/to_download to_download
assert to_download
finish

ftd="folder_to_download"
start "test download a folder"
mkdir -p $ftd/a/b/c
touch $ftd/a/1.txt
touch $ftd/a/2.txt
touch $ftd/a/b/c/3.txt
gsutil cp -r $ftd gs://gsg-uat/$testid/$fd
rm -rf $ftd
../gsg cp -r gs://gsg-uat/$testid/$ftd $ftd
assert $ftd/a/1.txt
assert $ftd/a/2.txt
assert $ftd/a/b/c/3.txt
finish

start "test rsync a remote folder to local"
cp $ftd/a/1.txt rsync_a_1.txt
echo "whocares" > $ftd/a/1.txt
../gsg rsync -r gs://gsg-uat/$testid/$ftd $ftd
same $ftd/a/1.txt rsync_a_1.txt
finish


start "leaving $testbase"
popd
finish

start "cleanup test ground"
rm -rf uat_temp || true
gsutil ls  gs://gsg-uat | xargs -I {} gsutil -m rm -r {} || true
finish

finish "everything OK!"
