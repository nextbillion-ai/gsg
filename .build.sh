set -e

rm -rf build

mkdir -p build/darwin-arm64
mv bar/bar.go bar/bar.go_backup && mv bar/bar.go_darwin_arm64 bar/bar.go
GOOS=darwin GOARCH=arm64 go build -o build/darwin-arm64/gsg
mv bar/bar.go bar/bar.go_darwin_arm64 && mv bar/bar.go_backup bar/bar.go

mkdir -p build/darwin-amd64
GOOS=darwin GOARCH=amd64 go build -o build/darwin-amd64/gsg

mkdir -p build/linux-arm64
GOOS=linux GOARCH=arm64 go build -o build/linux-arm64/gsg

mkdir -p build/linux-amd64
GOOS=linux GOARCH=amd64 go build -o build/linux-amd64/gsg
