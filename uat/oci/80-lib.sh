# The library API (lib/lock, lib/object), which ten programs outside this repo
# import. The cli cases cover none of it: it has its own scheme switches and
# its own entry points.

start "lib: object read, write, list and delete against oci"

cat > libprobe.go <<'GO'
package main

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/nextbillion-ai/gsg/lib/object"
)

func main() {
	base := os.Args[1]
	o, err := object.New(base + "/a.txt")
	if err != nil {
		fmt.Println("FAIL new:", err)
		os.Exit(1)
	}
	if err = o.Write(strings.NewReader("library round trip\n")); err != nil {
		fmt.Println("FAIL write:", err)
		os.Exit(1)
	}
	var buf bytes.Buffer
	if err = o.Read(&buf); err != nil {
		fmt.Println("FAIL read:", err)
		os.Exit(1)
	}
	if buf.String() != "library round trip\n" {
		fmt.Printf("FAIL content: %q\n", buf.String())
		os.Exit(1)
	}
	d, _ := object.New(base + "/sub/b.txt")
	if err = d.Write(strings.NewReader("nested\n")); err != nil {
		fmt.Println("FAIL nested write:", err)
		os.Exit(1)
	}
	l, _ := object.New(base)
	items, lerr := l.List(true)
	if lerr != nil || len(items) != 2 {
		fmt.Printf("FAIL list: %d items err=%v\n", len(items), lerr)
		os.Exit(1)
	}
	subs, serr := l.SubPaths()
	if serr != nil || len(subs) != 1 {
		fmt.Printf("FAIL subpaths: %v err=%v\n", subs, serr)
		os.Exit(1)
	}
	if err = o.Delete(); err != nil {
		fmt.Println("FAIL delete:", err)
		os.Exit(1)
	}
	// The sentinel is what consumers check for, so a deleted object has to
	// come back as ErrObjectNotFound rather than a raw sdk error.
	var b2 bytes.Buffer
	if err = o.Read(&b2); err != object.ErrObjectNotFound {
		fmt.Printf("FAIL read-after-delete: want ErrObjectNotFound, got %v\n", err)
		os.Exit(1)
	}
	// PutObject needs the body's length and checksum before it sends
	// anything. A seekable reader -- everything above -- is measured in place
	// and rewound. Anything else is spooled to a temporary file, and that
	// route is only reached by a reader that cannot seek, so it needs its own
	// case or it is never exercised against the service at all.
	payload := make([]byte, 3*1024*1024)
	for i := range payload {
		payload[i] = byte(i * 7 % 251)
	}
	before := spoolFiles()
	big, _ := object.New(base + "/spooled.bin")
	if err = big.Write(notSeekable{bytes.NewReader(payload)}); err != nil {
		fmt.Println("FAIL spooled write:", err)
		os.Exit(1)
	}
	var b3 bytes.Buffer
	if err = big.Read(&b3); err != nil {
		fmt.Println("FAIL spooled read:", err)
		os.Exit(1)
	}
	if !bytes.Equal(b3.Bytes(), payload) {
		fmt.Printf("FAIL spooled content: %d bytes back, wanted %d\n", b3.Len(), len(payload))
		os.Exit(1)
	}
	// The spool file must not outlive the upload.
	if after := spoolFiles(); after > before {
		fmt.Printf("FAIL: %d spool file(s) left behind\n", after-before)
		os.Exit(1)
	}
	_ = big.Delete()

	_ = d.Delete()
	fmt.Println("PASS")
}

// notSeekable hides the Seek a bytes.Reader would otherwise offer.
type notSeekable struct{ r io.Reader }

func (n notSeekable) Read(p []byte) (int, error) { return n.r.Read(p) }

func spoolFiles() int {
	m, _ := filepath.Glob(filepath.Join(os.TempDir(), "gsg-oci-put-*"))
	return len(m)
}
GO

out=$(cd .. && go run "$OLDPWD/libprobe.go" "$remote_base/libobj" 2>&1 | tail -1) || true
assertEq "the library round trips an object on oci, seekable and spooled" "$out" "PASS"
rm -f libprobe.go

finish

start "lib: distributed lock against oci"

cat > lockprobe.go <<'GO'
package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/nextbillion-ai/gsg/lib/lock"
)

func main() {
	url := os.Args[1]
	d, err := lock.NewWithUrl(url)
	if err != nil {
		fmt.Println("FAIL NewWithUrl:", err)
		os.Exit(1)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err = d.Lock(ctx, 300*time.Second); err != nil {
		fmt.Println("FAIL lock:", err)
		os.Exit(1)
	}
	// A second holder must not get in. lib/lock keeps the id in memory rather
	// than in a /tmp receipt, so this is a genuinely separate holder --
	// unlike the cli, it does not share state (see TODO item 24).
	d2, _ := lock.NewWithUrl(url)
	ctx2, cancel2 := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel2()
	if err = d2.Lock(ctx2, 300*time.Second); err == nil {
		fmt.Println("FAIL: a second holder acquired the same lock")
		os.Exit(1)
	}
	// And it must not be able to release one it never took.
	if err = d2.Unlock(); err == nil {
		fmt.Println("FAIL: a non-holder released the lock")
		os.Exit(1)
	}
	if err = d.Unlock(); err != nil {
		fmt.Println("FAIL unlock:", err)
		os.Exit(1)
	}
	fmt.Println("PASS")
}
GO

out=$(cd .. && go run "$OLDPWD/lockprobe.go" "$remote_base/liblock/a.lock" 2>&1 | tail -1) || true
assertEq "the library lock is exclusive and only its holder releases it" "$out" "PASS"
rm -f lockprobe.go

finish
