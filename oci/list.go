package oci

import "github.com/nextbillion-ai/gsg/system"

// List returns the objects and sub-directories under prefix.
func (o *OCI) List(bucket, prefix string, recursive bool) ([]*system.FileObject, error) {
	return nil, errNotImplemented("List")
}
