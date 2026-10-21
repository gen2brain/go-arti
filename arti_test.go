package arti

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"
)

const torProjectOnion = "2gzyxa5ihm7nsggfxnu52rck2vv4rvmdlkiu3zzui5du4xyclen53wid.onion"

func TestEmbeddedWasm(t *testing.T) {
	wasm, err := gunzip(wasmGz)
	if err != nil {
		t.Fatalf("gunzip embedded wasm: %v", err)
	}
	if !bytes.HasPrefix(wasm, []byte("\x00asm")) {
		t.Fatalf("embedded blob is not a wasm module (bad magic)")
	}
}

func TestDialBadAddr(t *testing.T) {
	c, err := New(context.Background(), "")
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	for _, addr := range []string{"example.com", "example.com:notaport", "example.com:70000"} {
		if _, err := c.Dial("tcp", addr); err == nil {
			t.Errorf("Dial(%q) = nil error, want error", addr)
		}
	}
}

func TestPoolRoundRobin(t *testing.T) {
	p := &Pool{clients: []*Client{{}, {}, {}}}
	seen := map[*Client]int{}
	for i := 0; i < 9; i++ {
		seen[p.Client()]++
	}
	if len(seen) != 3 {
		t.Fatalf("round-robin used %d of 3 clients", len(seen))
	}
	for c, n := range seen {
		if n != 3 {
			t.Errorf("client %p used %d times, want 3", c, n)
		}
	}
}

func TestIsolationGroups(t *testing.T) {
	single := &Client{circuits: 1}
	for i := 0; i < 3; i++ {
		if g := single.isolationGroup(); g != 0 {
			t.Fatalf("single circuit group = %d, want 0", g)
		}
	}
	c := &Client{circuits: 3}
	seen := map[uint32]int{}
	for i := 0; i < 9; i++ {
		seen[c.isolationGroup()]++
	}
	for g := uint32(1); g <= 3; g++ {
		if seen[g] != 3 {
			t.Errorf("group %d used %d times, want 3 (%v)", g, seen[g], seen)
		}
	}
}

func newBootstrapped(t *testing.T, cacheDir string) *Client {
	t.Helper()
	c, err := New(context.Background(), cacheDir)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Bootstrap(); err != nil {
		c.Close()
		t.Fatalf("bootstrap: %v", err)
	}
	return c
}

func bootstrapAborted(t *testing.T, c *Client, stop func()) {
	t.Helper()
	done := make(chan error, 1)
	go func() { done <- c.Bootstrap() }()
	time.Sleep(500 * time.Millisecond)
	stop()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("Bootstrap succeeded, want an abort error")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Bootstrap did not return after stop")
	}
}

func TestBootstrapCancel(t *testing.T) {
	if testing.Short() {
		t.Skip("network test; skipped under -short")
	}
	ctx, cancel := context.WithCancel(context.Background())
	c, err := New(ctx, "")
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	bootstrapAborted(t, c, cancel)
}

func TestCloseDuringBootstrap(t *testing.T) {
	if testing.Short() {
		t.Skip("network test; skipped under -short")
	}
	c, err := New(context.Background(), "")
	if err != nil {
		t.Fatal(err)
	}
	bootstrapAborted(t, c, func() { c.Close() })
}

func TestFetchOverTor(t *testing.T) {
	if testing.Short() {
		t.Skip("network test; skipped under -short")
	}
	c := newBootstrapped(t, "")
	defer c.Close()

	for _, url := range []string{"http://example.com/", "https://example.com/"} {
		resp, err := c.HTTPClient().Get(url)
		if err != nil {
			t.Errorf("GET %s: %v", url, err)
			continue
		}
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		resp.Body.Close()
		if resp.StatusCode != 200 {
			t.Errorf("GET %s: status %s", url, resp.Status)
		}
		if !strings.Contains(string(body), "Example Domain") {
			t.Errorf("GET %s: unexpected body", url)
		}
	}
}

func TestOnionConnect(t *testing.T) {
	if testing.Short() {
		t.Skip("network test; skipped under -short")
	}
	c := newBootstrapped(t, "")
	defer c.Close()

	conn, err := c.Dial("tcp", torProjectOnion+":80")
	if err != nil {
		t.Fatalf("dial onion: %v", err)
	}
	defer conn.Close()
	fmt.Fprintf(conn, "GET / HTTP/1.0\r\nHost: %s\r\nConnection: close\r\n\r\n", torProjectOnion)
	status, err := bufio.NewReader(conn).ReadString('\n')
	if err != nil {
		t.Fatalf("read onion response: %v", err)
	}
	if !strings.HasPrefix(status, "HTTP/1.1") {
		t.Errorf("unexpected onion response: %q", status)
	}
}

func TestOnionKeyPersistence(t *testing.T) {
	if testing.Short() {
		t.Skip("network test; skipped under -short")
	}
	dir := t.TempDir()

	launch := func() string {
		c := newBootstrapped(t, dir)
		defer c.Close()
		svc, err := c.ServeOnion("persist-test", "127.0.0.1:1")
		if err != nil {
			t.Fatalf("serve onion: %v", err)
		}
		defer svc.Close()
		return svc.Address()
	}

	a1 := launch()
	a2 := launch()
	if !strings.HasSuffix(a1, ".onion") {
		t.Fatalf("not an onion address: %q", a1)
	}
	if a1 != a2 {
		t.Errorf("onion address not persistent across runs: %q != %q", a1, a2)
	}
}

func TestReadDeadline(t *testing.T) {
	if testing.Short() {
		t.Skip("network test; skipped under -short")
	}
	c := newBootstrapped(t, "")
	defer c.Close()

	conn, err := c.Dial("tcp", "example.com:80")
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	start := time.Now()
	_, err = conn.Read(make([]byte, 64))
	if err == nil {
		t.Fatal("read returned nil error, want timeout")
	}
	ne, ok := err.(net.Error)
	if !ok || !ne.Timeout() {
		t.Fatalf("read error = %v, want net.Error with Timeout()", err)
	}
	if !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Errorf("read error %v does not wrap os.ErrDeadlineExceeded", err)
	}
	if elapsed := time.Since(start); elapsed > 10*time.Second {
		t.Errorf("read blocked %s, deadline was 2s", elapsed)
	}
}

func TestServeOnionRoundTrip(t *testing.T) {
	if testing.Short() {
		t.Skip("network test; skipped under -short")
	}

	const want = "hello from the onion service"
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, want)
	}))
	defer srv.Close()
	target := srv.Listener.Addr().String()

	host := newBootstrapped(t, "")
	defer host.Close()
	svc, err := host.ServeOnion("roundtrip", target)
	if err != nil {
		t.Fatalf("serve onion: %v", err)
	}
	defer svc.Close()
	onion := svc.Address()
	if !strings.HasSuffix(onion, ".onion") {
		t.Fatalf("bad onion address: %q", onion)
	}
	t.Logf("serving %s -> %s", onion, target)

	client := newBootstrapped(t, "")
	defer client.Close()
	hc := client.HTTPClient()

	url := "http://" + onion + "/"
	deadline := time.Now().Add(4 * time.Minute)
	var lastErr error
	for attempt := 1; time.Now().Before(deadline); attempt++ {
		resp, err := hc.Get(url)
		if err != nil {
			t.Logf("attempt %d: %v", attempt, err)
			lastErr = err
			time.Sleep(6 * time.Second)
			continue
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode == 200 && strings.Contains(string(body), want) {
			return
		}
		lastErr = fmt.Errorf("status %s, body %q", resp.Status, body)
		time.Sleep(6 * time.Second)
	}
	t.Fatalf("onion round-trip did not succeed within deadline: %v", lastErr)
}

func TestPoolConcurrent(t *testing.T) {
	if testing.Short() {
		t.Skip("network test; skipped under -short")
	}
	p, err := NewPool(context.Background(), "", 2)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	client := p.HTTPClient()
	var wg sync.WaitGroup
	errs := make(chan error, 4)
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			resp, err := client.Get("http://example.com/")
			if err != nil {
				errs <- err
				return
			}
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
			if resp.StatusCode != 200 {
				errs <- fmt.Errorf("status %s", resp.Status)
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
}

func TestCircuitsConcurrent(t *testing.T) {
	if testing.Short() {
		t.Skip("network test; skipped under -short")
	}
	c, err := New(context.Background(), "", WithCircuits(3))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if err := c.Bootstrap(); err != nil {
		t.Fatalf("bootstrap: %v", err)
	}

	client := c.HTTPClient()
	var wg sync.WaitGroup
	errs := make(chan error, 6)
	for i := 0; i < 6; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			resp, err := client.Get("http://example.com/")
			if err != nil {
				errs <- err
				return
			}
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
			if resp.StatusCode != 200 {
				errs <- fmt.Errorf("status %s", resp.Status)
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
}
