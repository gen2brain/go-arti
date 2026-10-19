package arti

import (
	"context"
	"errors"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"

	"github.com/tetratelabs/wazero"
)

// Pool is a set of independent Clients used round-robin, so dials run in parallel.
type Pool struct {
	clients []*Client
	next    atomic.Uint64
	cache   wazero.CompilationCache
}

// NewPool creates and bootstraps size Clients, each in its own subdirectory of
// cacheDir, or in a temporary directory if cacheDir is empty.
func NewPool(ctx context.Context, cacheDir string, size int, opts ...Option) (*Pool, error) {
	if size < 1 {
		size = 1
	}
	p := &Pool{clients: make([]*Client, size), cache: wazero.NewCompilationCache()}
	opts = append(slices.Clip(opts), func(o *options) { o.cache = p.cache })

	for i := range size {
		dir := ""
		if cacheDir != "" {
			dir = filepath.Join(cacheDir, strconv.Itoa(i))
			if err := os.MkdirAll(dir, 0o700); err != nil {
				p.Close()
				return nil, err
			}
		}
		c, err := New(ctx, dir, opts...)
		if err != nil {
			p.Close()
			return nil, err
		}
		p.clients[i] = c
	}

	if err := p.bootstrap(); err != nil {
		p.Close()
		return nil, err
	}
	return p, nil
}

func (p *Pool) bootstrap() error {
	var cold []*Client
	seed := -1
	for i, c := range p.clients {
		switch {
		case !c.hasDirCache():
			cold = append(cold, c)
		case seed < 0:
			seed = i
		}
	}
	if seed < 0 {
		seed, cold = 0, cold[1:]
	}
	rest := p.clients
	if len(cold) > 0 {
		s := p.clients[seed]
		if err := s.Bootstrap(); err != nil {
			return err
		}
		for _, c := range cold {
			if err := s.copyDirCache(c); err != nil {
				return err
			}
		}
		rest = slices.Delete(slices.Clone(rest), seed, seed+1)
	}

	errs := make([]error, len(rest))
	var wg sync.WaitGroup
	for i, c := range rest {
		wg.Go(func() { errs[i] = c.Bootstrap() })
	}
	wg.Wait()
	return errors.Join(errs...)
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

// RoundTrip implements http.RoundTripper on the next Client in the pool.
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
	if p.cache != nil {
		return p.cache.Close(context.Background())
	}
	return nil
}
