package oci

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"hash/crc32"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	ocicommon "github.com/oracle/oci-go-sdk/v65/common"
	"github.com/oracle/oci-go-sdk/v65/objectstorage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/nextbillion-ai/gsg/common"
)

// rangedBucket is a client whose service answers GetObject with the range of
// content it was asked for.
func rangedBucket(t *testing.T, content []byte) bucketRef {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var start, end int
		if _, err := fmt.Sscanf(r.Header.Get("Range"), "bytes=%d-%d", &start, &end); err != nil {
			start, end = 0, len(content)-1
		}
		w.Header().Set("Content-Length", fmt.Sprint(end-start+1))
		w.WriteHeader(http.StatusPartialContent)
		_, _ = w.Write(content[start : end+1])
	}))
	t.Cleanup(srv.Close)
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	keyPEM := string(pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)}))
	provider := ocicommon.NewRawConfigurationProvider("ocid1.tenancy.oc1..t", "ocid1.user.oc1..u", "us-ashburn-1", "aa:bb", keyPEM, nil)
	c, err := objectstorage.NewObjectStorageClientWithConfigurationProvider(provider)
	require.NoError(t, err)
	c.Host = srv.URL
	return bucketRef{c: &c, ns: "ns", name: "b", region: "us-ashburn-1"}
}

// A chunk sums what it writes in either mode, so the download is settled, and
// its CRC32C cached, without reading the file back.
func TestFetchChunkSumsWhatItWritesInEitherMode(t *testing.T) {
	content := []byte("0123456789abcdefghijklm")
	ref := rangedBucket(t, content)
	for _, gentle := range []bool{false, true} {
		for _, chunk := range []struct{ off, length int64 }{{0, 23}, {0, 7}, {7, 7}, {21, 2}} {
			path := filepath.Join(t.TempDir(), "o")
			require.NoError(t, os.WriteFile(path, make([]byte, len(content)), 0o644))
			sum, n, err := (&OCI{}).fetchChunk(ref, "o", nil, path, chunk.off, chunk.length, nil, gentle)
			require.NoError(t, err, "gentle=%v %+v", gentle, chunk)
			assert.Equal(t, chunk.length, n)
			assert.Equal(t, crc32.Checksum(content[chunk.off:chunk.off+chunk.length], common.Castagnoli), sum, "gentle=%v %+v", gentle, chunk)
		}
	}
}
