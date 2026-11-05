package oci

import (
	"errors"
	"testing"

	"github.com/oracle/oci-go-sdk/v65/objectstorage"
	"github.com/stretchr/testify/assert"
)

func TestNoCRC32CStoredOnlyMeansAbsent(t *testing.T) {
	assert.True(t, noCRC32CStored(nil))
	assert.True(t, noCRC32CStored(strp("")))
	assert.False(t, noCRC32CStored(strp(crc32cToBase64(7))))
	assert.False(t, noCRC32CStored(strp("!!notbase64")), "unreadable is unknown, not absent")
}

func TestCRCLookupSeparatesAbsentFromUnknown(t *testing.T) {
	head := func(crc *string, err error, gone bool) func() (*objectstorage.HeadObjectResponse, error) {
		return func() (*objectstorage.HeadObjectResponse, error) {
			if err != nil || gone {
				return nil, err
			}
			return &objectstorage.HeadObjectResponse{OpcContentCrc32c: crc}, nil
		}
	}
	cases := map[string]struct {
		head func() (*objectstorage.HeadObjectResponse, error)
		want crcLookup
	}{
		"stores one":       {head(strp(crc32cToBase64(7)), nil, false), crcLookup{crc: 7, known: true}},
		"stores none":      {head(nil, nil, false), crcLookup{absent: true}},
		"stores empty":     {head(strp(""), nil, false), crcLookup{absent: true}},
		"unreadable value": {head(strp("!!notbase64"), nil, false), crcLookup{}},
		"request failed":   {head(nil, errors.New("boom"), false), crcLookup{}},
		"object vanished":  {head(nil, nil, true), crcLookup{}},
	}
	for name, c := range cases {
		assert.Equal(t, c.want, lazyCRCLookup("oci://b/k", c.head)(), name)
	}
}

// Both questions about an object share one HEAD, and asking neither costs none.
func TestCRCLookupHeadsAtMostOnce(t *testing.T) {
	heads := 0
	lookup := lazyCRCLookup("oci://b/k", func() (*objectstorage.HeadObjectResponse, error) {
		heads++
		return &objectstorage.HeadObjectResponse{}, nil
	})
	assert.Equal(t, 0, heads)
	lookup()
	lookup()
	assert.Equal(t, 1, heads)
}

// A listing entry opts in, and listing it costs no request. An entry with no
// name cannot be asked about, so it stays out.
func TestSummaryOptsInOnlyWithAName(t *testing.T) {
	o := &OCI{}
	named := o.summaryToAttrs("b@ns.r", objectstorage.ObjectSummary{Name: strp("k"), Size: int64p(1)})
	assert.NotNil(t, named.CalcCRC32C)
	assert.NotNil(t, named.NoCRC32C)
	nameless := o.summaryToAttrs("b@ns.r", objectstorage.ObjectSummary{Size: int64p(1)})
	assert.Nil(t, nameless.CalcCRC32C)
	assert.Nil(t, nameless.NoCRC32C)
}
