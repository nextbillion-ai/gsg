set -e

./.build.sh
go run main.go -m rsync -r -d build gs://static.nextbillion.io/tools/gsg/latest
go run main.go -m rsync -r -d build gs://static.nextbillion.io/tools/gsg/$CI_COMMIT_TAG
