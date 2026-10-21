// Package arti is a CGo-free Tor client: arti-client compiled to wasm32-wasip1, run with wazero.
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
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/tetratelabs/wazero"
	"github.com/tetratelabs/wazero/api"
	"github.com/tetratelabs/wazero/imports/wasi_snapshot_preview1"
)

//go:embed lib/arti.wasm.gz
var wasmGz []byte

// Client is an embedded Tor client. It is safe for concurrent use; calls into it are serialized.
type Client struct {
	rt   wazero.Runtime
	mod  api.Module
	ctx  context.Context
	dir  string
	temp bool

	callMu     sync.Mutex
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

	circuits uint32
	group    atomic.Uint32

	serveMu    sync.Mutex
	serveCount int
	serveStop  chan struct{}
	serveDown  bool
	pumpWg     sync.WaitGroup
}

// Option configures a Client or Pool.
type Option func(*options)

type options struct {
	stderr   io.Writer
	cache    wazero.CompilationCache
	circuits int
}

// WithStderr sets where diagnostic output goes; the default is os.Stderr.
func WithStderr(w io.Writer) Option {
	return func(o *options) { o.stderr = w }
}

// WithCircuits spreads a Client's connections round-robin over n isolation
// groups. Connections in different groups never share a Tor circuit, so
// parallel streams are not limited by the bandwidth of one circuit. The
// default is 1: all connections may share circuits.
func WithCircuits(n int) Option {
	return func(o *options) { o.circuits = n }
}

// New creates a Client that keeps its state in cacheDir, or in a temporary
// directory if cacheDir is empty. Cancelling ctx aborts Bootstrap and closes the Client.
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

	rc := wazero.NewRuntimeConfig()
	if o.cache != nil {
		rc = rc.WithCompilationCache(o.cache)
	}
	rt := wazero.NewRuntimeWithConfig(ctx, rc)
	wasi_snapshot_preview1.MustInstantiate(ctx, rt)

	c := &Client{rt: rt, ctx: ctx, dir: root, temp: temp, reactor: newReactor(), circuits: uint32(max(o.circuits, 1))}

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

// OnionService is a running onion service.
type OnionService struct {
	c    *Client
	addr string
	once sync.Once
}

// Address returns the .onion address.
func (o *OnionService) Address() string { return o.addr }

// Close stops serving this onion service.
func (o *OnionService) Close() error {
	o.once.Do(o.c.stopPump)
	return nil
}

// ServeOnion launches an onion service proxied to targetAddr ("host:port"). The
// nickname keeps its .onion address across runs when cacheDir is persistent.
func (c *Client) ServeOnion(nickname, targetAddr string) (*OnionService, error) {
	addr, err := c.launchOnion(nickname, targetAddr)
	if err != nil {
		return nil, err
	}
	c.startPump()
	return &OnionService{c: c, addr: addr}, nil
}

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

	const outCap = 128
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

func (c *Client) cacheDir() string { return filepath.Join(c.dir, "arti", "cache") }

func (c *Client) hasDirCache() bool {
	_, err := os.Stat(filepath.Join(c.cacheDir(), "dir.sqlite3"))
	return err == nil
}

func (c *Client) copyDirCache(dst *Client) error {
	c.callMu.Lock()
	defer c.callMu.Unlock()
	if err := os.RemoveAll(dst.cacheDir()); err != nil {
		return err
	}
	return os.CopyFS(dst.cacheDir(), os.DirFS(c.cacheDir()))
}

// Bootstrap connects to the Tor network. It only needs to be called once.
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

const connectPollInterval = 40 * time.Millisecond

// DialContext opens a stream to addr ("host:port", host may be a .onion); network is ignored.
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

func (c *Client) isolationGroup() uint32 {
	if c.circuits <= 1 {
		return 0
	}
	return c.group.Add(1)%c.circuits + 1
}

func (c *Client) connectStart(host string, port int) (uint32, error) {
	c.callMu.Lock()
	defer c.callMu.Unlock()
	ptr, err := c.writeBytes([]byte(host))
	if err != nil {
		return 0, err
	}
	defer c.freeBytes(ptr, len(host))
	res, err := c.connStart.Call(c.ctx, uint64(ptr), uint64(len(host)), uint64(port), uint64(c.isolationGroup()))
	if err != nil {
		return 0, fmt.Errorf("arti: connect: %w", err)
	}
	if int64(res[0]) < 0 {
		return 0, fmt.Errorf("arti: could not connect to %s:%d: %s", host, port, c.lastError())
	}
	return uint32(res[0]), nil
}

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

// HTTPClient returns an http.Client routed through Tor.
func (c *Client) HTTPClient() *http.Client {
	return &http.Client{Transport: c.RoundTripper()}
}

// RoundTripper returns an http.RoundTripper that uses a new Tor stream per request.
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

type bodyCloser struct {
	io.ReadCloser
	conn net.Conn
}

func (b bodyCloser) Close() error {
	err := b.ReadCloser.Close()
	b.conn.Close()
	return err
}

// Close closes the Client and removes a temporary directory.
func (c *Client) Close() error {
	c.reactor.Close()
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
	if c.temp {
		os.RemoveAll(c.dir)
	}
	return err
}

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

type conn struct {
	c      *Client
	id     uint32
	remote string

	mu            sync.Mutex
	readDeadline  time.Time
	writeDeadline time.Time
}

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
