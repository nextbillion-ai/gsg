package common

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"sync/atomic"
	"time"
)

// DefaultStallTimeout is how long a connection may go without moving a byte
// before its read or write fails.
//
// No deadline on a whole request fits a transfer: objects run from bytes to
// 80 GB, and a deadline sized to the largest would leave a small request hung
// for an hour. A healthy connection, however slow, moves something every few
// seconds; the longest legitimate silence is the service committing an upload
// or a compose after the last byte of the request. Two minutes is what
// jam-core settled on for the same SDK and buckets (a 2.1 GB object uploads
// in about 25 s there), and the oci client already gives up on response
// headers after one.
const DefaultStallTimeout = 2 * time.Minute

// StallTimeout is the stall limit for the clients gsg builds itself. It is read
// when a client is built, which happens once per process (per bucket for s3,
// per region for oci), so a caller changes it before its first transfer. Zero
// or less turns the guard off.
var StallTimeout = DefaultStallTimeout

// DialContextFunc is the signature of http.Transport.DialContext.
type DialContextFunc func(ctx context.Context, network, addr string) (net.Conn, error)

// StallGuardDialContext wraps dial so that every connection it returns fails a
// read or a write that has waited stall without moving a byte: headers that
// never come, a body that stops mid-way and an upload the peer stops taking all
// fail after stall, however long a healthy transfer on the same connection runs.
// A stall of zero or less returns dial unchanged.
//
// The read deadline moves forward on every write as well as every read. The
// transport keeps a read pending on a pooled connection from the moment it goes
// idle, so a deadline set only when that read started would count the idle time
// against the next request. A pooled connection idle for longer than stall is
// closed by the same deadline, which only costs a new dial.
//
// Over HTTP/2 the streams share the connection, so one stream that stops while
// others move is not caught; gsg's own clients use HTTP/1.1 for gs.
func StallGuardDialContext(dial DialContextFunc, stall time.Duration) DialContextFunc {
	if stall <= 0 {
		return dial
	}
	return func(ctx context.Context, network, addr string) (net.Conn, error) {
		conn, err := dial(ctx, network, addr)
		if err != nil {
			return nil, err
		}
		if err := conn.SetReadDeadline(time.Now().Add(stall)); err != nil {
			_ = conn.Close()
			return nil, err
		}
		return &stallConn{Conn: conn, stall: stall}, nil
	}
}

type stallConn struct {
	net.Conn
	stall time.Duration
	// stalled is the first stall seen, which every later failure on the
	// connection reports
	stalled atomic.Pointer[StallError]
}

func (c *stallConn) Read(b []byte) (int, error) {
	if err := c.Conn.SetReadDeadline(time.Now().Add(c.stall)); err != nil {
		return 0, c.wrap("read", err)
	}
	n, err := c.Conn.Read(b)
	return n, c.wrap("read", err)
}

func (c *stallConn) Write(b []byte) (int, error) {
	deadline := time.Now().Add(c.stall)
	if err := c.Conn.SetWriteDeadline(deadline); err != nil {
		return 0, c.wrap("write", err)
	}
	// a request going out restarts the wait for its answer
	if err := c.Conn.SetReadDeadline(deadline); err != nil {
		return 0, c.wrap("write", err)
	}
	n, err := c.Conn.Write(b)
	return n, c.wrap("write", err)
}

// wrap turns a deadline into a StallError, and so does any error after one.
// The transport reads and writes a connection from two goroutines: when one
// side stalls it closes the connection, and the other side then fails with
// "use of closed network connection". Which of the two errors a request returns
// is a race, and the second is net.ErrClosed, which the GCS library retries for
// as long as its context lives; an upload the service stopped taking came back
// as that about half the time.
func (c *stallConn) wrap(op string, err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, os.ErrDeadlineExceeded) {
		se := &StallError{Op: op, Stall: c.stall, Err: err}
		if c.stalled.CompareAndSwap(nil, se) {
			return se
		}
	}
	if se := c.stalled.Load(); se != nil {
		return se
	}
	return err
}

// StallError reports a connection on which a read or write made no progress
// for Stall.
//
// It deliberately does not unwrap to the deadline error underneath: that one
// reports itself Temporary, and the GCS library retries a Temporary error for
// as long as its context lives, which for gsg is forever, so a peer that stays
// silent would hold the transfer one stall at a time without end. As a final
// error it reaches gsg's own bounded retry instead.
type StallError struct {
	Op    string
	Stall time.Duration
	Err   error
}

func (e *StallError) Error() string {
	return fmt.Sprintf("connection stalled: no %s progress for %s: %v", e.Op, e.Stall, e.Err)
}

// Timeout lets callers that test for net.Error timeouts treat a stall as one.
func (e *StallError) Timeout() bool { return true }

// Temporary is part of net.Error; see the type's comment for why it is false.
func (e *StallError) Temporary() bool { return false }
