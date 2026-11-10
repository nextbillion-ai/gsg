package cmd

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/base64"
	"encoding/binary"
	"encoding/pem"
	"errors"
	"fmt"
	"hash/crc32"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"

	"cloud.google.com/go/storage"
	"github.com/aws/aws-sdk-go-v2/aws"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/nextbillion-ai/gsg/gcs"
	"github.com/nextbillion-ai/gsg/oci"
	"github.com/nextbillion-ai/gsg/s3"
	"github.com/nextbillion-ai/gsg/system"
	ocicommon "github.com/oracle/oci-go-sdk/v65/common"
	"github.com/oracle/oci-go-sdk/v65/objectstorage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/option"
)

// The one object every fake store holds. It sits a directory below the
// prefix rsync is pointed at, so a regular file in the destination can stand
// where that directory has to go.
const (
	fakeObject  = "dir/sub/o.bin"
	fakeContent = "01234567"
	childEnv    = "GSG_TRANSFER_CHILD"
	ociRegion   = "us-phoenix-1"
)

// fakeStore answers metadata and listings for fakeObject, and its first media
// read with status (0 serves it). Only the first fails when transient is set.
type fakeStore struct {
	status    int
	transient bool
	// reason goes into a gs error body, as the service gives it
	reason string
	media  atomic.Int64
}

// readFault reports whether this media read is to fail, and with what.
func (f *fakeStore) readFault() int {
	n := f.media.Add(1)
	if f.status == 0 || (f.transient && n > 1) {
		return 0
	}
	return f.status
}

func listing(prefix, delimiter string) (objects, prefixes []string) {
	if !strings.HasPrefix(fakeObject, prefix) {
		return nil, nil
	}
	rest := fakeObject[len(prefix):]
	if delimiter != "" {
		if i := strings.Index(rest, delimiter); i >= 0 {
			return nil, []string{prefix + rest[:i+1]}
		}
	}
	return []string{fakeObject}, nil
}

func serveRange(w http.ResponseWriter, r *http.Request) {
	body := []byte(fakeContent)
	start, end := 0, len(body)-1
	if rg := r.Header.Get("Range"); rg != "" {
		_, _ = fmt.Sscanf(rg, "bytes=%d-%d", &start, &end)
		end = min(end, len(body)-1)
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(body)))
		w.Header().Set("Content-Length", strconv.Itoa(end-start+1))
		w.WriteHeader(http.StatusPartialContent)
	} else {
		w.Header().Set("Content-Length", strconv.Itoa(len(body)))
	}
	_, _ = w.Write(body[start : end+1])
}

// resetFault is a status no service sends: the read starts, then the
// connection drops half way through the body.
const resetFault = -1

// serveCut answers a read and drops the connection half way through the body.
func serveCut(w http.ResponseWriter, r *http.Request) {
	body := []byte(fakeContent)
	w.Header().Set("Content-Length", strconv.Itoa(len(body)))
	if r.Header.Get("Range") != "" {
		w.Header().Set("Content-Range", fmt.Sprintf("bytes 0-%d/%d", len(body)-1, len(body)))
		w.WriteHeader(http.StatusPartialContent)
	}
	_, _ = w.Write(body[:len(body)/2])
	w.(http.Flusher).Flush()
	conn, _, err := w.(http.Hijacker).Hijack()
	if err == nil {
		_ = conn.Close()
	}
}

func crc32cB64() string {
	var b [4]byte
	binary.BigEndian.PutUint32(b[:], crc32.Checksum([]byte(fakeContent), crc32.MakeTable(crc32.Castagnoli)))
	return base64.StdEncoding.EncodeToString(b[:])
}

func gsHandler(f *fakeStore) http.HandlerFunc {
	obj := fmt.Sprintf(`{"bucket":"b","name":%q,"size":"%d","generation":"7","crc32c":%q,"updated":"2026-01-02T03:04:05Z"}`,
		fakeObject, len(fakeContent), crc32cB64())
	return func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		if q.Get("alt") == "json" {
			w.Header().Set("Content-Type", "application/json")
			if r.URL.Path == "/b/b/o" {
				objects, prefixes := listing(q.Get("prefix"), q.Get("delimiter"))
				items := make([]string, 0, len(objects))
				for range objects {
					items = append(items, obj)
				}
				quoted := make([]string, 0, len(prefixes))
				for _, p := range prefixes {
					quoted = append(quoted, strconv.Quote(p))
				}
				fmt.Fprintf(w, `{"kind":"storage#objects","items":[%s],"prefixes":[%s]}`, strings.Join(items, ","), strings.Join(quoted, ","))
				return
			}
			if r.URL.Path == "/b/b/o/"+fakeObject {
				fmt.Fprint(w, obj)
				return
			}
			w.WriteHeader(http.StatusNotFound)
			fmt.Fprint(w, `{"error":{"code":404,"message":"No such object"}}`)
			return
		}
		if code := f.readFault(); code == resetFault {
			serveCut(w, r)
			return
		} else if code != 0 {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(code)
			fmt.Fprintf(w, `{"error":{"code":%d,"message":"injected","errors":[{"reason":%q}]}}`, code, f.reason)
			return
		}
		serveRange(w, r)
	}
}

func s3Handler(f *fakeStore) http.HandlerFunc {
	const modified = "Sun, 05 Nov 2026 02:14:22 GMT"
	return func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		key := strings.TrimPrefix(r.URL.Path, "/b/")
		switch {
		case r.URL.Path == "/b" || r.URL.Path == "/b/":
			objects, prefixes := listing(q.Get("prefix"), q.Get("delimiter"))
			var b strings.Builder
			b.WriteString(`<ListBucketResult><IsTruncated>false</IsTruncated>`)
			for _, o := range objects {
				fmt.Fprintf(&b, `<Contents><Key>%s</Key><Size>%d</Size><LastModified>2026-11-05T02:14:22.000Z</LastModified></Contents>`, o, len(fakeContent))
			}
			for _, p := range prefixes {
				fmt.Fprintf(&b, `<CommonPrefixes><Prefix>%s</Prefix></CommonPrefixes>`, p)
			}
			b.WriteString(`</ListBucketResult>`)
			fmt.Fprint(w, b.String())
		case q.Has("attributes"):
			if key != fakeObject {
				w.WriteHeader(http.StatusNotFound)
				fmt.Fprint(w, `<Error><Code>NoSuchKey</Code></Error>`)
				return
			}
			w.Header().Set("Last-Modified", modified)
			fmt.Fprintf(w, `<GetObjectAttributesResponse><ObjectSize>%d</ObjectSize></GetObjectAttributesResponse>`, len(fakeContent))
		default:
			if code := f.readFault(); code == resetFault {
				serveCut(w, r)
				return
			} else if code != 0 {
				w.WriteHeader(code)
				fmt.Fprintf(w, `<Error><Code>Injected%d</Code></Error>`, code)
				return
			}
			w.Header().Set("Last-Modified", modified)
			serveRange(w, r)
		}
	}
}

func ociHandler(f *fakeStore) http.HandlerFunc {
	const modified = "Sun, 05 Nov 2026 02:14:22 GMT"
	base := "/n/ns/b/b"
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		path := r.URL.Path
		switch {
		case path == base || path == base+"/":
			fmt.Fprint(w, `{"name":"b","namespace":"ns","compartmentId":"c","createdBy":"u","timeCreated":"2026-01-02T03:04:05Z","etag":"e"}`)
		case path == base+"/o" || path == base+"/o/":
			q := r.URL.Query()
			objects, prefixes := listing(q.Get("prefix"), q.Get("delimiter"))
			items := make([]string, 0, len(objects))
			for _, o := range objects {
				items = append(items, fmt.Sprintf(`{"name":%q,"size":%d,"timeModified":"2026-11-05T02:14:22Z"}`, o, len(fakeContent)))
			}
			quoted := make([]string, 0, len(prefixes))
			for _, p := range prefixes {
				quoted = append(quoted, strconv.Quote(p))
			}
			fmt.Fprintf(w, `{"objects":[%s],"prefixes":[%s]}`, strings.Join(items, ","), strings.Join(quoted, ","))
		case strings.HasPrefix(path, base+"/o/"):
			if strings.TrimPrefix(path, base+"/o/") != fakeObject {
				w.WriteHeader(http.StatusNotFound)
				if r.Method != http.MethodHead {
					fmt.Fprint(w, `{"code":"ObjectNotFound","message":"no such object"}`)
				}
				return
			}
			w.Header().Set("ETag", "etag-1")
			w.Header().Set("Last-Modified", modified)
			w.Header().Set("opc-content-crc32c", crc32cB64())
			if r.Method == http.MethodHead {
				w.Header().Set("Content-Length", strconv.Itoa(len(fakeContent)))
				return
			}
			if code := f.readFault(); code == resetFault {
				serveCut(w, r)
				return
			} else if code != 0 {
				w.WriteHeader(code)
				fmt.Fprintf(w, `{"code":"Injected%d","message":"injected"}`, code)
				return
			}
			w.Header().Set("Content-Type", "application/octet-stream")
			serveRange(w, r)
		default:
			w.WriteHeader(http.StatusNotImplemented)
		}
	}
}

// registerFake points the scheme's backend at url, inside the child.
func registerFake(t *testing.T, backend, url string) {
	switch backend {
	case "gs":
		c, err := storage.NewClient(context.Background(), option.WithEndpoint(url), option.WithoutAuthentication())
		require.NoError(t, err)
		system.Register(gcs.NewWithClient(c))
	case "s3":
		system.Register(s3.NewWithClients(map[string]*awss3.Client{"b": awss3.New(awss3.Options{
			Region:       "us-east-1",
			BaseEndpoint: aws.String(url),
			UsePathStyle: true,
			Credentials:  aws.AnonymousCredentials{},
		})}))
	case "oci":
		key, err := rsa.GenerateKey(rand.Reader, 2048)
		require.NoError(t, err)
		pemKey := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)})
		provider := ocicommon.NewRawConfigurationProvider("ocid1.tenancy.oc1..t", "ocid1.user.oc1..u", ociRegion, "aa:bb", string(pemKey), nil)
		c, err := objectstorage.NewObjectStorageClientWithConfigurationProvider(provider)
		require.NoError(t, err)
		c.Host = url
		system.Register(oci.NewWithClient(ociRegion, "ns", &c))
	}
}

// TestTransferChild is the gsg process TestTransferFailures runs: the real
// command, with the backend for one scheme pointed at a fake store.
func TestTransferChild(t *testing.T) {
	spec := os.Getenv(childEnv)
	if spec == "" {
		t.Skip("run as a child of TestTransferFailures")
	}
	parts := strings.Split(spec, "\x1f")
	registerFake(t, parts[0], parts[1])
	rootCmd.SetArgs(parts[2:])
	if err := Execute(); err != nil {
		os.Exit(2)
	}
	os.Exit(0)
}

func remoteRoot(backend string) string {
	if backend == "oci" {
		return "oci://b@" + ociRegion
	}
	return backend + "://b"
}

// cp and rsync, run as a separate gsg process against a fake store of each
// backend: an error no attempt can change is tried once, printed once and
// exits 1, and a 503 or a connection dropped mid-body is still retried to
// success.
func TestTransferFailures(t *testing.T) {
	type fault struct {
		name      string
		status    int
		transient bool
		local     string // "notdir" or "readonly": the destination, not the store, fails
		reason    string
		only      string // the one backend the fault applies to
		skip      string // a backend it does not apply to
	}
	faults := []fault{
		{name: "403", status: 403, reason: "forbidden"},
		// gs reads the generation its lookup found, so a 404 there is an
		// overwrite, not a missing object; see "404 once"
		{name: "404", status: 404, skip: "gs"},
		{name: "not a directory", local: "notdir"},
		{name: "permission denied", local: "readonly"},
		{name: "503 once", status: 503, transient: true},
		{name: "reset once", status: resetFault, transient: true},
		// an object overwritten under a pinned read: gs answers 404 for the
		// generation, oci 412 for the ETag; another attempt reads the new one
		{name: "404 once", status: 404, transient: true, only: "gs"},
		{name: "412 once", status: 412, transient: true},
		{name: "403 rate limit once", status: 403, transient: true, reason: "rateLimitExceeded", only: "gs"},
		{name: "403 user rate limit once", status: 403, transient: true, reason: "userRateLimitExceeded", only: "gs"},
	}
	handlers := map[string]func(*fakeStore) http.HandlerFunc{"gs": gsHandler, "s3": s3Handler, "oci": ociHandler}
	for _, backend := range []string{"gs", "s3", "oci"} {
		for _, command := range []string{"cp", "rsync"} {
			for _, fl := range faults {
				if (fl.only != "" && fl.only != backend) || fl.skip == backend {
					continue
				}
				t.Run(backend+"/"+command+"/"+fl.name, func(t *testing.T) {
					if fl.local == "readonly" && os.Geteuid() == 0 {
						t.Skip("root writes to a read-only directory regardless of its mode")
					}
					store := &fakeStore{status: fl.status, transient: fl.transient, reason: fl.reason}
					srv := httptest.NewServer(handlers[backend](store))
					defer srv.Close()

					dir := t.TempDir()
					dst := filepath.Join(dir, "out")
					require.NoError(t, os.Mkdir(dst, 0o755))
					switch fl.local {
					case "notdir":
						// a file where the object's directory has to be created
						require.NoError(t, os.WriteFile(filepath.Join(dst, "sub"), nil, 0o644))
					case "readonly":
						require.NoError(t, os.Mkdir(filepath.Join(dst, "sub"), 0o555))
						defer func() { _ = os.Chmod(filepath.Join(dst, "sub"), 0o755) }()
					}
					want := filepath.Join(dst, "sub", "o.bin")
					args := []string{"cp", remoteRoot(backend) + "/" + fakeObject, want}
					if command == "rsync" {
						args = []string{"rsync", "-r", remoteRoot(backend) + "/dir", dst}
					}

					child := exec.Command(os.Args[0], "-test.run=^TestTransferChild$")
					child.Env = append(os.Environ(), childEnv+"="+strings.Join(append([]string{backend, srv.URL}, args...), "\x1f"))
					out, err := child.CombinedOutput()
					code := 0
					var ee *exec.ExitError
					if errors.As(err, &ee) {
						code = ee.ExitCode()
					} else {
						require.NoError(t, err)
					}

					if fl.transient {
						assert.Equal(t, 0, code, "%s", out)
						got, rerr := os.ReadFile(want)
						require.NoError(t, rerr, "%s", out)
						assert.Equal(t, fakeContent, string(got))
						assert.Equal(t, int64(2), store.media.Load(), "the read was tried again")
						return
					}
					assert.Equal(t, 1, code, "%s", out)
					assert.NotContains(t, string(out), "invalid argument")
					marker := fl.name
					switch {
					case fl.local != "":
						assert.Zero(t, store.media.Load(), "nothing is fetched for a file that has nowhere to go")
					default:
						assert.Equal(t, int64(1), store.media.Load(), "one attempt\n%s", out)
					}
					// a log line starts with its timestamp; an SDK's message runs on
					// over lines of its own that repeat the status
					said := 0
					for _, line := range strings.Split(string(out), "\n") {
						if strings.HasPrefix(line, "[") && strings.Contains(line, marker) {
							said++
						}
					}
					assert.Equal(t, 1, said, "the reason is printed once\n%s", out)
				})
			}
		}
	}
}
