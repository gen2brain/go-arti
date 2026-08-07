package arti

import (
	"context"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"sync/atomic"
)

// Pool is a set of independent Tor Client instances, each its own
// single-threaded wasm instance, so dials distributed round-robin across them
// run in parallel instead of serializing on one. The cost is one bootstrap and
// one wasm instance (tens of MB) per Client.
type Pool struct {
	clients []*Client
	next    atomic.Uint64
}

// NewPool creates and bootstraps size Client instances concurrently. A non-empty
// cacheDir gives each Client its own persistent subdirectory; otherwise each uses
// a temporary directory.
func NewPool(ctx context.Context, cacheDir string, size int, opts ...Option) (*Pool, error) {
	if size < 1 {
		size = 1
	}
	p := &Pool{clients: make([]*Client, size)}

	type result struct {
		i   int
		c   *Client
		err error
	}
	ch := make(chan result, size)
	for i := 0; i < size; i++ {
		go func(i int) {
			dir := ""
			if cacheDir != "" {
				dir = filepath.Join(cacheDir, strconv.Itoa(i))
				if err := os.MkdirAll(dir, 0o700); err != nil {
					ch <- result{i, nil, err}
					return
				}
			}
			c, err := New(ctx, dir, opts...)
			if err == nil {
				err = c.Bootstrap()
			}
			ch <- result{i, c, err}
		}(i)
	}

	var firstErr error
	for k := 0; k < size; k++ {
		r := <-ch
		if r.err != nil {
			if firstErr == nil {
				firstErr = r.err
			}
			if r.c != nil {
				r.c.Close()
			}
			continue
		}
		p.clients[r.i] = r.c
	}
	if firstErr != nil {
		p.Close()
		return nil, firstErr
	}
	return p, nil
}

// Client returns the next Client in round-robin order.
func (p *Pool) Client() *Client {
	n := p.next.Add(1) - 1
	return p.clients[int(n%uint64(len(p.clients)))]
}

// DialContext dials addr through the next Client in the pool.
func (p *Pool) DialContext(ctx context.Context, network, addr string) (net.Conn, error) {
	return p.Client().DialContext(ctx, network, addr)
}

// Dial is DialContext with a background context.
func (p *Pool) Dial(network, addr string) (net.Conn, error) {
	return p.DialContext(context.Background(), network, addr)
}

// HTTPClient returns an http.Client that spreads requests across the pool.
func (p *Pool) HTTPClient() *http.Client {
	return &http.Client{Transport: p}
}

// RoundTrip implements http.RoundTripper, dispatching each request to the next
// Client in the pool so concurrent requests run in parallel.
func (p *Pool) RoundTrip(req *http.Request) (*http.Response, error) {
	return p.Client().RoundTripper().RoundTrip(req)
}

// Close closes every Client in the pool.
func (p *Pool) Close() error {
	for _, c := range p.clients {
		if c != nil {
			c.Close()
		}
	}
	return nil
}
