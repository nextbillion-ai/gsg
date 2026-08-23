package oci

// Copy duplicates an object within OCI Object Storage.
func (o *OCI) Copy(srcBucket, srcPrefix, dstBucket, dstPrefix string) error {
	return errNotImplemented("Copy")
}

// Move copies an object and then removes the source.
func (o *OCI) Move(srcBucket, srcPrefix, dstBucket, dstPrefix string) error {
	return errNotImplemented("Move")
}

// Delete removes an object.
func (o *OCI) Delete(bucket, prefix string) error {
	return errNotImplemented("Delete")
}
