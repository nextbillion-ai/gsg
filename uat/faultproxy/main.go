// faultproxy is an HTTP CONNECT proxy that breaks connections on command, for
// the fault-injection cases recorded in TODO.md (items 35, 36 and 39).
//
// gsg's GCS client honours HTTPS_PROXY, so pointing it here routes every
// request through a tunnel this process controls. TLS is not terminated: the
// proxy sees only byte counts per tunnel, which is enough to cut, refuse or
// black-hole traffic at a chosen point of a transfer.
//
//	go build -o /tmp/faultproxy ./uat/faultproxy && /tmp/faultproxy &
//	HTTPS_PROXY=http://127.0.0.1:18080 gsg cp ...
//
// Control endpoint, plain HTTP on -ctl:
//
//	GET /stats               {"up":..,"down":..,"active":..,"total":..,"refused":..,"resets":..,"mode":..}
//	GET /reset               zero the byte counters
//	GET /mode?set=pass       forward normally
//	GET /mode?set=blip       reset every open tunnel once, then pass
//	GET /mode?set=refuse     reset every open tunnel and refuse new ones (502) until told otherwise
//	GET /mode?set=dropdown   keep forwarding client->server but discard server->client:
//	                         the service receives and commits, the client never hears
//	GET /resetrefuse?n=N     reset every open tunnel, refuse the next N, then pass
//
// Faults apply only to tunnels whose target contains -match (the storage API by
// default); token requests pass untouched, so credentials keep working.
//
// Where the client runs changes what a reset looks like. On macOS it arrives as
// ECONNRESET or EPIPE. From a container under Docker Desktop it arrived as a
// bare EOF, most likely because the desktop's forwarding turns the RST into a
// close.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

var (
	listen = flag.String("listen", "127.0.0.1:18080", "proxy address")
	ctl    = flag.String("ctl", "127.0.0.1:18081", "control address")
	match  = flag.String("match", "storage.googleapis.com", "faults apply to CONNECT targets containing this")
)

type tunnel struct {
	client net.Conn
	server net.Conn
	faulty bool
}

var (
	mu      sync.Mutex
	mode    = "pass"
	tunnels = map[int64]*tunnel{}
	nextID  int64

	up      atomic.Int64 // client -> server bytes through faulty tunnels
	down    atomic.Int64 // server -> client bytes delivered through faulty tunnels
	total   atomic.Int64
	refused atomic.Int64
	resets  atomic.Int64
	// refuseNext > 0 refuses that many more CONNECTs, then passes again
	refuseNext atomic.Int64
)

func currentMode() string {
	mu.Lock()
	defer mu.Unlock()
	return mode
}

func setMode(m string) {
	mu.Lock()
	mode = m
	mu.Unlock()
}

func takeRefusal() bool {
	for {
		n := refuseNext.Load()
		if n <= 0 {
			return false
		}
		if refuseNext.CompareAndSwap(n, n-1) {
			return true
		}
	}
}

// resetFaulty closes every faulty tunnel with an RST rather than a FIN: a reset
// is what a dropped NAT entry or a dead middlebox produces, and it cannot be
// mistaken for a clean end of response.
func resetFaulty() int {
	mu.Lock()
	var victims []*tunnel
	for _, t := range tunnels {
		if t.faulty {
			victims = append(victims, t)
		}
	}
	mu.Unlock()
	for _, t := range victims {
		if tc, ok := t.client.(*net.TCPConn); ok {
			_ = tc.SetLinger(0)
		}
		_ = t.client.Close()
		_ = t.server.Close()
	}
	resets.Add(int64(len(victims)))
	return len(victims)
}

func handleConnect(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodConnect {
		http.Error(w, "CONNECT only", http.StatusMethodNotAllowed)
		return
	}
	faulty := strings.Contains(r.Host, *match)
	if faulty && (currentMode() == "refuse" || takeRefusal()) {
		refused.Add(1)
		http.Error(w, "refused by faultproxy", http.StatusBadGateway)
		return
	}
	server, err := net.DialTimeout("tcp", r.Host, 15*time.Second)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	hj, ok := w.(http.Hijacker)
	if !ok {
		_ = server.Close()
		http.Error(w, "no hijack", http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
	client, _, err := hj.Hijack()
	if err != nil {
		_ = server.Close()
		return
	}
	mu.Lock()
	nextID++
	id := nextID
	tunnels[id] = &tunnel{client: client, server: server, faulty: faulty}
	mu.Unlock()
	total.Add(1)

	var wg sync.WaitGroup
	wg.Add(2)
	go func() { // client -> server
		defer wg.Done()
		_, _ = io.Copy(countingWriter{server, faulty}, client)
		_ = server.Close()
	}()
	go func() { // server -> client
		defer wg.Done()
		buf := make([]byte, 32*1024)
		for {
			n, err := server.Read(buf)
			if n > 0 && !(faulty && currentMode() == "dropdown") {
				if _, werr := client.Write(buf[:n]); werr != nil {
					break
				}
				if faulty {
					down.Add(int64(n))
				}
			}
			if err != nil {
				break
			}
		}
		_ = client.Close()
	}()
	wg.Wait()
	mu.Lock()
	delete(tunnels, id)
	mu.Unlock()
}

type countingWriter struct {
	w      io.Writer
	faulty bool
}

func (c countingWriter) Write(p []byte) (int, error) {
	n, err := c.w.Write(p)
	if c.faulty {
		up.Add(int64(n))
	}
	return n, err
}

func control() *http.ServeMux {
	m := http.NewServeMux()
	reply := func(w http.ResponseWriter, msg string) {
		log.Print(msg)
		fmt.Fprintln(w, msg)
	}
	m.HandleFunc("/stats", func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		active, md := len(tunnels), mode
		mu.Unlock()
		_ = json.NewEncoder(w).Encode(map[string]any{
			"up": up.Load(), "down": down.Load(), "active": active, "total": total.Load(),
			"refused": refused.Load(), "resets": resets.Load(), "mode": md,
		})
	})
	m.HandleFunc("/reset", func(w http.ResponseWriter, r *http.Request) {
		up.Store(0)
		down.Store(0)
		reply(w, "counters zeroed")
	})
	m.HandleFunc("/mode", func(w http.ResponseWriter, r *http.Request) {
		switch set := r.URL.Query().Get("set"); set {
		case "pass", "dropdown":
			setMode(set)
			reply(w, "mode="+set)
		case "refuse":
			setMode(set)
			reply(w, fmt.Sprintf("mode=refuse, reset %d tunnel(s)", resetFaulty()))
		case "blip":
			n := resetFaulty()
			setMode("pass")
			reply(w, fmt.Sprintf("blip: reset %d tunnel(s), mode=pass", n))
		default:
			http.Error(w, fmt.Sprintf("unknown mode %q", set), http.StatusBadRequest)
		}
	})
	m.HandleFunc("/resetrefuse", func(w http.ResponseWriter, r *http.Request) {
		var n int64
		_, _ = fmt.Sscan(r.URL.Query().Get("n"), &n)
		refuseNext.Store(n)
		setMode("pass")
		reply(w, fmt.Sprintf("resetrefuse: reset %d tunnel(s), refusing the next %d, mode=pass", resetFaulty(), n))
	})
	return m
}

func main() {
	flag.Parse()
	go func() { log.Fatal(http.ListenAndServe(*ctl, control())) }()
	log.Printf("faultproxy on %s, control on %s, faults for %q", *listen, *ctl, *match)
	log.Fatal(http.ListenAndServe(*listen, http.HandlerFunc(handleConnect)))
}
