# Gsutil Go (gsg)

This is a tool for operating objects on gcs with command line mode.

-Cli app could found on gcs:
` gs://static.nextbillion.io/gsg/latest`
` gs://static.nextbillion.io/gsg/{version}`

-For accessing gcp need to set an env pointing to google credential json file:
`GOOGLE_APPLICATION_CREDENTIALS`

-For updating dependencies:
add into import line, then run `go mod tidy`, to add into local vendor by `go mod vendor`

-If want to manually build the app:
run script `.build.sh` then will create `build` folder

-Check help tips:
run `gsg` or `gsg help`