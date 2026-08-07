// Package arti is a self-contained, CGo-free Tor client for Go. It embeds the
// Rust arti-client compiled to wasm32-wasip1 and runs it with wazero, exposing a
// net.Conn Dialer, an http.Client, and onion-service hosting.
//
// A Client is one wasm instance with shared memory, so calls into it are
// serialized. Use a Pool for concurrency.
package arti

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/rand"
	"crypto/tls"
	_ "embed"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/tetratelabs/wazero"
	"github.com/tetratelabs/wazero/api"
	"github.com/tetratelabs/wazero/imports/wasi_snapshot_preview1"
)

//go:embed lib/arti.wasm.gz
var wasmGz []byte

// Client is a bootstrapped embedded Tor client. Create one with New, call
// Bootstrap, then use DialContext / HTTPClient. It is safe for concurrent use.
type Client struct {
	rt   wazero.Runtime
	mod  api.Module
	ctx  context.Context
	root string

	callMu  sync.Mutex // serializes all calls into the wasm module
	alloc      api.Function
	free       api.Function
	boot       api.Function
	connStart  api.Function
	connPoll   api.Function
	connCancel api.Function
	read       api.Function
	write      api.Function
	close      api.Function
	onion      api.Function
	lasterr    api.Function
	pump       api.Function

	reactor *reactor

	serveMu    sync.Mutex // guards the serving pump lifecycle
	serveCount int
	serveStop  chan struct{}
	serveDown  bool // set by Close; blocks further pump restarts
	pumpWg     sync.WaitGroup
}

// Option configures a Client or Pool.
type Option func(*options)

type options struct {
	stderr io.Writer
}

// WithStderr routes the embedded module's diagnostic output (bootstrap and
// per-connection errors) to w instead of os.Stderr.
func WithStderr(w io.Writer) Option {
	return func(o *options) { o.stderr = w }
}

// New instantiates the embedded Tor module without bootstrapping (call Bootstrap
// next). A non-empty cacheDir persists Tor's directory cache across runs;
// otherwise a temporary directory is used.
func New(ctx context.Context, cacheDir string, opts ...Option) (*Client, error) {
	o := options{stderr: os.Stderr}
	for _, opt := range opts {
		opt(&o)
	}

	wasm, err := gunzip(wasmGz)
	if err != nil {
		return nil, err
	}

	root := cacheDir
	temp := false
	if root == "" {
		root, err = os.MkdirTemp("", "go-arti-*")
		if err != nil {
			return nil, err
		}
		temp = true
	}

	rt := wazero.NewRuntime(ctx)
	wasi_snapshot_preview1.MustInstantiate(ctx, rt)

	c := &Client{rt: rt, ctx: ctx, reactor: newReactor()}
	if temp {
		c.root = root
	}

	if err := c.registerHost(ctx); err != nil {
		rt.Close(ctx)
		return nil, err
	}

	cfg := wazero.NewModuleConfig().
		WithName("arti").
		WithArgs("arti").
		WithStderr(o.stderr).
		WithSysWalltime().
		WithSysNanotime().
		WithSysNanosleep().
		WithRandSource(rand.Reader).
		WithFSConfig(wazero.NewFSConfig().WithDirMount(root, "/"))

	c.mod, err = rt.InstantiateWithConfig(ctx, wasm, cfg)
	if err != nil {
		rt.Close(ctx)
		return nil, fmt.Errorf("arti: instantiate: %w", err)
	}

	c.alloc = c.mod.ExportedFunction("arti_alloc")
	c.free = c.mod.ExportedFunction("arti_free")
	c.boot = c.mod.ExportedFunction("arti_bootstrap")
	c.connStart = c.mod.ExportedFunction("arti_connect_start")
	c.connPoll = c.mod.ExportedFunction("arti_connect_poll")
	c.connCancel = c.mod.ExportedFunction("arti_connect_cancel")
	c.read = c.mod.ExportedFunction("arti_read")
	c.write = c.mod.ExportedFunction("arti_write")
	c.close = c.mod.ExportedFunction("arti_close")
	c.onion = c.mod.ExportedFunction("arti_onion_launch")
	c.lasterr = c.mod.ExportedFunction("arti_last_error")
	c.pump = c.mod.ExportedFunction("arti_pump")
	return c, nil
}

// lastError returns the most recent error string recorded inside the wasm
// module, or "" if none. Call with callMu held.
func (c *Client) lastError() string {
	const cap = 512
	res, err := c.alloc.Call(c.ctx, cap)
	if err != nil {
		return ""
	}
	ptr := uint32(res[0])
	defer c.freeBytes(ptr, cap)

	r, err := c.lasterr.Call(c.ctx, uint64(ptr), cap)
	if err != nil {
		return ""
	}
	n := int64(r[0])
	if n <= 0 {
		return ""
	}
	msg, ok := c.mod.Memory().Read(ptr, uint32(n))
	if !ok {
		return ""
	}
	return string(msg)
}

// OnionService is a running onion service that reverse-proxies incoming streams
// to a local target address. Close it to stop serving.
type OnionService struct {
	c    *Client
	addr string
	once sync.Once
}

// Address returns the service's .onion address (including the ".onion" suffix).
func (o *OnionService) Address() string { return o.addr }

// Close stops serving this onion service.
func (o *OnionService) Close() error {
	o.once.Do(o.c.stopPump)
	return nil
}

// ServeOnion launches an onion service that reverse-proxies each incoming stream
// to targetAddr ("host:port"). With a persistent cacheDir the same nickname keeps
// the same .onion address across runs.
func (c *Client) ServeOnion(nickname, targetAddr string) (*OnionService, error) {
	addr, err := c.launchOnion(nickname, targetAddr)
	if err != nil {
		return nil, err
	}
	c.startPump()
	return &OnionService{c: c, addr: addr}, nil
}

// launchOnion starts the service in the wasm module and returns its address.
func (c *Client) launchOnion(nickname, targetAddr string) (string, error) {
	c.callMu.Lock()
	defer c.callMu.Unlock()

	nptr, err := c.writeBytes([]byte(nickname))
	if err != nil {
		return "", err
	}
	defer c.freeBytes(nptr, len(nickname))

	tptr, err := c.writeBytes([]byte(targetAddr))
	if err != nil {
		return "", err
	}
	defer c.freeBytes(tptr, len(targetAddr))

	const outCap = 128 // a v3 .onion address is ~62 bytes
	res, err := c.alloc.Call(c.ctx, outCap)
	if err != nil {
		return "", err
	}
	optr := uint32(res[0])
	defer c.freeBytes(optr, outCap)

	r, err := c.onion.Call(c.ctx, uint64(nptr), uint64(len(nickname)),
		uint64(tptr), uint64(len(targetAddr)), uint64(optr), outCap)
	if err != nil {
		return "", err
	}
	n := int64(r[0])
	if n < 0 {
		return "", fmt.Errorf("arti: serve onion %q failed: %s", nickname, c.lastError())
	}
	addr, ok := c.mod.Memory().Read(optr, uint32(n))
	if !ok {
		return "", fmt.Errorf("arti: memory read out of range")
	}
	return string(addr), nil
}

// startPump refcounts the pump goroutine up, starting it on the first service.
func (c *Client) startPump() {
	c.serveMu.Lock()
	defer c.serveMu.Unlock()
	if c.serveDown {
		return
	}
	c.serveCount++
	if c.serveCount == 1 {
		c.serveStop = make(chan struct{})
		c.pumpWg.Add(1)
		go c.pumpLoop(c.serveStop)
	}
}

// stopPump refcounts the pump goroutine down, stopping it on the last service.
func (c *Client) stopPump() {
	c.serveMu.Lock()
	defer c.serveMu.Unlock()
	if c.serveCount == 0 {
		return
	}
	c.serveCount--
	if c.serveCount == 0 && c.serveStop != nil {
		close(c.serveStop)
		c.serveStop = nil
	}
}

// pumpLoop drives the executor in bounded slices so service accept loops make
// progress; each slice holds callMu at most pumpBudgetMs so Dials interleave.
func (c *Client) pumpLoop(stop chan struct{}) {
	defer c.pumpWg.Done()
	const pumpBudgetMs = 100
	for {
		select {
		case <-stop:
			return
		default:
		}
		c.callMu.Lock()
		_, err := c.pump.Call(c.ctx, uint64(pumpBudgetMs))
		c.callMu.Unlock()
		if err != nil {
			return
		}
	}
}

// Bootstrap connects to the Tor network and downloads directory information.
// This can take tens of seconds. It only needs to be called once.
func (c *Client) Bootstrap() error {
	c.callMu.Lock()
	defer c.callMu.Unlock()
	res, err := c.boot.Call(c.ctx)
	if err != nil {
		return fmt.Errorf("arti: bootstrap: %w", err)
	}
	if int32(res[0]) != 0 {
		return fmt.Errorf("arti: bootstrap failed: %s", c.lastError())
	}
	return nil
}

// connectPollInterval is how often DialContext polls a pending connect. The
// executor advances all pending connects during the pump between polls.
const connectPollInterval = 40 * time.Millisecond

// DialContext opens a stream to addr ("host:port") through Tor; host may be a
// clearnet name/IP or a v3 .onion. The network argument is ignored. The connect
// runs on the shared executor, so many Dials proceed concurrently.
func (c *Client) DialContext(ctx context.Context, _, addr string) (net.Conn, error) {
	host, portStr, err := net.SplitHostPort(addr)
	if err != nil {
		return nil, err
	}
	port, err := strconv.Atoi(portStr)
	if err != nil || port < 0 || port > 65535 {
		return nil, fmt.Errorf("arti: invalid port %q", portStr)
	}

	id, err := c.connectStart(host, port)
	if err != nil {
		return nil, err
	}

	c.startPump()
	defer c.stopPump()

	ticker := time.NewTicker(connectPollInterval)
	defer ticker.Stop()
	for {
		switch st, msg := c.connectPoll(id); {
		case st == 1:
			return &conn{c: c, id: id, remote: addr}, nil
		case st < 0:
			return nil, fmt.Errorf("arti: could not connect to %s: %s", addr, msg)
		}
		select {
		case <-ctx.Done():
			c.connectCancel(id)
			return nil, ctx.Err()
		case <-ticker.C:
		}
	}
}

// connectStart begins a connect and returns its stream id.
func (c *Client) connectStart(host string, port int) (uint32, error) {
	c.callMu.Lock()
	defer c.callMu.Unlock()
	ptr, err := c.writeBytes([]byte(host))
	if err != nil {
		return 0, err
	}
	defer c.freeBytes(ptr, len(host))
	res, err := c.connStart.Call(c.ctx, uint64(ptr), uint64(len(host)), uint64(port))
	if err != nil {
		return 0, fmt.Errorf("arti: connect: %w", err)
	}
	if int64(res[0]) < 0 {
		return 0, fmt.Errorf("arti: could not connect to %s:%d: %s", host, port, c.lastError())
	}
	return uint32(res[0]), nil
}

// connectPoll returns 1 (connected), 0 (pending) or -1 (failed, with message).
func (c *Client) connectPoll(id uint32) (int32, string) {
	c.callMu.Lock()
	defer c.callMu.Unlock()
	res, err := c.connPoll.Call(c.ctx, uint64(id))
	if err != nil {
		return -1, err.Error()
	}
	st := int32(res[0])
	if st < 0 {
		return st, c.lastError()
	}
	return st, ""
}

func (c *Client) connectCancel(id uint32) {
	c.callMu.Lock()
	defer c.callMu.Unlock()
	c.connCancel.Call(c.ctx, uint64(id))
}

// Dial is DialContext with a background context.
func (c *Client) Dial(network, addr string) (net.Conn, error) {
	return c.DialContext(context.Background(), network, addr)
}

// HTTPClient returns an http.Client routed through Tor. It uses a
// single-request-per-connection RoundTripper (not http.Transport, whose
// concurrent goroutines serialize badly on the single-threaded module); https
// TLS is done by Go over the Tor stream.
func (c *Client) HTTPClient() *http.Client {
	return &http.Client{Transport: c.RoundTripper()}
}

// RoundTripper returns an http.RoundTripper that dials each request over a fresh
// Tor stream and reads the response on the calling goroutine.
func (c *Client) RoundTripper() http.RoundTripper {
	return roundTripper{c}
}

type roundTripper struct{ c *Client }

func (rt roundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	port := req.URL.Port()
	if port == "" {
		if req.URL.Scheme == "https" {
			port = "443"
		} else {
			port = "80"
		}
	}

	conn, err := rt.c.DialContext(req.Context(), "tcp", net.JoinHostPort(req.URL.Hostname(), port))
	if err != nil {
		return nil, err
	}
	if req.URL.Scheme == "https" {
		tconn := tls.Client(conn, &tls.Config{ServerName: req.URL.Hostname()})
		if err := tconn.Handshake(); err != nil {
			conn.Close()
			return nil, err
		}
		conn = tconn
	}
	if err := req.Write(conn); err != nil {
		conn.Close()
		return nil, err
	}
	resp, err := http.ReadResponse(bufio.NewReader(conn), req)
	if err != nil {
		conn.Close()
		return nil, err
	}
	resp.Body = bodyCloser{ReadCloser: resp.Body, conn: conn}
	return resp, nil
}

// bodyCloser closes the underlying Tor stream when the response body is closed.
type bodyCloser struct {
	io.ReadCloser
	conn net.Conn
}

func (b bodyCloser) Close() error {
	err := b.ReadCloser.Close()
	b.conn.Close()
	return err
}

// Close tears down the wasm instance and removes any temporary cache directory.
func (c *Client) Close() error {
	c.reactor.Close() // unblock any in-flight read/write so the pump can stop
	c.serveMu.Lock()
	c.serveDown = true
	if c.serveStop != nil {
		close(c.serveStop)
		c.serveStop = nil
		c.serveCount = 0
	}
	c.serveMu.Unlock()
	c.pumpWg.Wait()

	err := c.rt.Close(c.ctx)
	if c.root != "" {
		os.RemoveAll(c.root)
	}
	return err
}

// --- memory helpers (call with callMu held) --------------------------------

func (c *Client) writeBytes(data []byte) (uint32, error) {
	res, err := c.alloc.Call(c.ctx, uint64(len(data)))
	if err != nil {
		return 0, fmt.Errorf("arti: alloc: %w", err)
	}
	ptr := uint32(res[0])
	if !c.mod.Memory().Write(ptr, data) {
		c.freeBytes(ptr, len(data))
		return 0, fmt.Errorf("arti: memory write out of range")
	}
	return ptr, nil
}

func (c *Client) freeBytes(ptr uint32, length int) {
	c.free.Call(c.ctx, uint64(ptr), uint64(length))
}

// --- net.Conn over a Tor stream --------------------------------------------

type conn struct {
	c      *Client
	id     uint32
	remote string

	mu            sync.Mutex
	readDeadline  time.Time
	writeDeadline time.Time
}

// timeoutMs returns the milliseconds until d, clamped to >= 1, or 0 (block
// forever) if d is zero. A deadline already in the past yields 1 (fail fast).
func timeoutMs(d time.Time) uint64 {
	if d.IsZero() {
		return 0
	}
	ms := time.Until(d).Milliseconds()
	if ms < 1 {
		return 1
	}
	return uint64(ms)
}

// timeoutError is a net.Error reporting a deadline overrun. It wraps
// os.ErrDeadlineExceeded so errors.Is(err, os.ErrDeadlineExceeded) holds.
type timeoutError struct{ op string }

func (e timeoutError) Error() string   { return "arti: " + e.op + ": " + os.ErrDeadlineExceeded.Error() }
func (e timeoutError) Timeout() bool   { return true }
func (e timeoutError) Temporary() bool { return true }
func (e timeoutError) Unwrap() error   { return os.ErrDeadlineExceeded }

func (t *conn) Read(p []byte) (int, error) {
	t.mu.Lock()
	timeout := timeoutMs(t.readDeadline)
	t.mu.Unlock()

	t.c.callMu.Lock()
	defer t.c.callMu.Unlock()

	res, err := t.c.alloc.Call(t.c.ctx, uint64(len(p)))
	if err != nil {
		return 0, err
	}
	ptr := uint32(res[0])
	defer t.c.freeBytes(ptr, len(p))

	r, err := t.c.read.Call(t.c.ctx, uint64(t.id), uint64(ptr), uint64(len(p)), timeout)
	if err != nil {
		return 0, err
	}
	n := int64(r[0])
	switch {
	case n == -4:
		return 0, timeoutError{"read"}
	case n < 0:
		return 0, fmt.Errorf("arti: read error: %s", t.c.lastError())
	case n == 0:
		return 0, io.EOF
	}
	data, ok := t.c.mod.Memory().Read(ptr, uint32(n))
	if !ok {
		return 0, fmt.Errorf("arti: memory read out of range")
	}
	return copy(p, data), nil
}

func (t *conn) Write(p []byte) (int, error) {
	t.mu.Lock()
	timeout := timeoutMs(t.writeDeadline)
	t.mu.Unlock()

	t.c.callMu.Lock()
	defer t.c.callMu.Unlock()

	ptr, err := t.c.writeBytes(p)
	if err != nil {
		return 0, err
	}
	defer t.c.freeBytes(ptr, len(p))

	r, err := t.c.write.Call(t.c.ctx, uint64(t.id), uint64(ptr), uint64(len(p)), timeout)
	if err != nil {
		return 0, err
	}
	n := int64(r[0])
	switch {
	case n == -4:
		return 0, timeoutError{"write"}
	case n < 0:
		return 0, fmt.Errorf("arti: write error: %s", t.c.lastError())
	}
	return int(n), nil
}

func (t *conn) Close() error {
	t.c.callMu.Lock()
	defer t.c.callMu.Unlock()
	_, err := t.c.close.Call(t.c.ctx, uint64(t.id))
	return err
}

func (t *conn) LocalAddr() net.Addr  { return torAddr("tor") }
func (t *conn) RemoteAddr() net.Addr { return torAddr(t.remote) }

// Deadlines are enforced per Read/Write by passing the remaining time to the
// wasm side, which races the operation against a timer.
func (t *conn) SetDeadline(tm time.Time) error {
	t.mu.Lock()
	t.readDeadline, t.writeDeadline = tm, tm
	t.mu.Unlock()
	return nil
}

func (t *conn) SetReadDeadline(tm time.Time) error {
	t.mu.Lock()
	t.readDeadline = tm
	t.mu.Unlock()
	return nil
}

func (t *conn) SetWriteDeadline(tm time.Time) error {
	t.mu.Lock()
	t.writeDeadline = tm
	t.mu.Unlock()
	return nil
}

type torAddr string

func (a torAddr) Network() string { return "tor" }
func (a torAddr) String() string  { return string(a) }

func gunzip(b []byte) ([]byte, error) {
	r, err := gzip.NewReader(bytes.NewReader(b))
	if err != nil {
		return nil, err
	}
	defer r.Close()
	return io.ReadAll(r)
}
