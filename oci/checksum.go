package oci

import (
	"fmt"

	"github.com/nextbillion-ai/gsg/common"
	"github.com/nextbillion-ai/gsg/logger"
)

// equalCRC32C compares a local file against the object's stored checksum.
//
// The second result says whether there was anything to compare with. OCI
// records a CRC32C only when the uploader asked for one, so an object written
// by another tool has none -- and reading that as zero would fail every such
// object rather than admit the check could not be made. This is the shape #47
// arrived at for s3, for the same reason.
func (o *OCI) equalCRC32C(localPath, bucket, object string) (equal, comparable bool, err error) {
	attrs, err := o.Attributes(bucket, object)
	if err != nil {
		return false, false, err
	}
	if attrs == nil {
		// Verification was asked for and the object is not there. That is a
		// failure, not something to pass over.
		return false, false, fmt.Errorf("cannot verify oci://%s/%s: no such object", bucket, object)
	}
	// Attributes cannot say "no checksum" -- TODO item 18 -- so the head is
	// repeated here to ask that question directly rather than guessing from a
	// zero.
	head, err := o.headObject(bucket, object)
	if err != nil {
		return false, false, err
	}
	if head == nil {
		return false, false, fmt.Errorf("cannot verify oci://%s/%s: no such object", bucket, object)
	}
	remote, stored := crc32cOf(head.OpcContentCrc32c)
	if !stored {
		return false, false, nil
	}
	local := common.GetFileCRC32C(localPath)
	logger.Info(module, "CRC32C checking of local[%s] and bucket[%s] prefix[%s] are [%d] with [%d].",
		localPath, bucket, object, local, remote)
	return local == remote, true, nil
}

// MustEqualCRC32C verifies the downloaded file when the flag is set.
func (o *OCI) MustEqualCRC32C(flag bool, localPath, bucket, object string) error {
	if !flag {
		return nil
	}
	equal, comparable, err := o.equalCRC32C(localPath, bucket, object)
	if err != nil {
		return err
	}
	if !comparable {
		// Nothing to check against. Saying so is the honest outcome: failing
		// would reject every object written without a CRC32C, and passing in
		// silence is what makes a -v flag worthless.
		logger.Info(module, "CRC32C checking skipped for bucket[%s] prefix[%s]: no CRC32C stored", bucket, object)
		return nil
	}
	if !equal {
		log := fmt.Sprintf("CRC32C checking failed of local[%s] and bucket[%s] prefix[%s].", localPath, bucket, object)
		logger.Info(module, log)
		return fmt.Errorf("%s", log)
	}
	logger.Info(module, "CRC32C checking success of local[%s] and bucket[%s] prefix[%s].", localPath, bucket, object)
	return nil
}
