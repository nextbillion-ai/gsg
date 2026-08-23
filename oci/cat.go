package oci

// Cat returns an object's bytes.
func (o *OCI) Cat(bucket, prefix string) ([]byte, error) {
	return nil, errNotImplemented("Cat")
}

// IsObject reports whether prefix names an object.
func (o *OCI) IsObject(bucket, prefix string) (bool, error) {
	return false, errNotImplemented("IsObject")
}

// IsDirectory reports whether prefix names a directory.
func (o *OCI) IsDirectory(bucket, prefix string) (bool, error) {
	return false, errNotImplemented("IsDirectory")
}
