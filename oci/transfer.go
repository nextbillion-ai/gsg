package oci

import "github.com/nextbillion-ai/gsg/system"

// Download fetches an object to dstFile, verifying it when forceChecksum is set.
func (o *OCI) Download(bucket, prefix, dstFile string, forceChecksum bool, ctx system.RunContext) error {
	return errNotImplemented("Download")
}

// Upload stores srcFile as an object.
func (o *OCI) Upload(srcFile, bucket, object string, ctx system.RunContext) error {
	return errNotImplemented("Upload")
}
