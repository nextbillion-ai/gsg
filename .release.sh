set -e

version=`go run main.go version | sed -e "s/gsg version //"`

./.build.sh
go run main.go -m rsync -r -d build gs://static.nextbillion.io/tools/gsg/latest
go run main.go -m rsync -r -d build gs://static.nextbillion.io/tools/gsg/$version
