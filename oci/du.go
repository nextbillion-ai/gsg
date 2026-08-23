package oci

import "github.com/nextbillion-ai/gsg/system"

// DiskUsage reports the size of everything under prefix.
func (o *OCI) DiskUsage(bucket, prefix string, recursive bool) ([]system.DiskUsage, error) {
	return nil, errNotImplemented("DiskUsage")
}
