package oci

import "github.com/nextbillion-ai/gsg/system"

// Attributes returns size, mtime and checksum for a single object.
func (o *OCI) Attributes(bucket, prefix string) (*system.Attrs, error) {
	return nil, errNotImplemented("Attributes")
}

// BatchAttributes returns attributes for everything under prefix.
func (o *OCI) BatchAttributes(bucket, prefix string, recursive bool) ([]*system.Attrs, error) {
	return nil, errNotImplemented("BatchAttributes")
}
