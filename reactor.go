package arti

import (
	"context"
	"io"
	"net"
	"sync"
	"time"

	"github.com/tetratelabs/wazero/api"
	"github.com/tetratelabs/wazero/sys"
)

type reactor struct {
	mu     sync.Mutex
	conns  map[uint32]*rconn
	next   uint32
	ready  chan struct{}
	closed bool
}

func (r *reactor) Close() {
	r.mu.Lock()
	r.closed = true
	for _, c := range r.conns {
		c.close()
	}
	r.mu.Unlock()
	r.signal()
}

type rconn struct {
	cancel context.CancelFunc
	mu     sync.Mutex
	nc     net.Conn
	buf    []byte
	eof    bool
	failed bool
}

func (c *rconn) conn() net.Conn {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.nc
}

func (c *rconn) close() {
	c.cancel()
	if nc := c.conn(); nc != nil {
		nc.Close()
	}
}

func newReactor() *reactor {
	return &reactor{conns: make(map[uint32]*rconn), next: 1, ready: make(chan struct{}, 1)}
}

func (r *reactor) signal() {
	select {
	case r.ready <- struct{}{}:
	default:
	}
}

func (r *reactor) isClosed() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.closed
}

func (r *reactor) get(id uint32) *rconn {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.conns[id]
}

func (r *reactor) dial(ctx context.Context, c *rconn, addr string) {
	d := net.Dialer{Timeout: connectTimeout}
	nc, err := d.DialContext(ctx, "tcp", addr)
	c.mu.Lock()
	if err != nil || ctx.Err() != nil {
		c.failed = true
		c.mu.Unlock()
		if nc != nil {
			nc.Close()
		}
		r.signal()
		return
	}
	c.nc = nc
	c.mu.Unlock()
	r.signal()
	r.pump(c)
}

func (r *reactor) pump(c *rconn) {
	b := make([]byte, 32*1024)
	for {
		n, err := c.nc.Read(b)
		if n > 0 {
			c.mu.Lock()
			c.buf = append(c.buf, b[:n]...)
			c.mu.Unlock()
			r.signal()
		}
		if err != nil {
			c.mu.Lock()
			if err == io.EOF {
				c.eof = true
			} else {
				c.failed = true
			}
			c.mu.Unlock()
			r.signal()
			return
		}
	}
}

const connectTimeout = 30 * time.Second

func (c *Client) registerHost(ctx context.Context) error {
	r := c.reactor
	_, err := c.rt.NewHostModuleBuilder("arti_host").
		NewFunctionBuilder().WithFunc(func(_ context.Context, mod api.Module, ptr, length uint32) int64 {
		addr, ok := mod.Memory().Read(ptr, length)
		if !ok {
			return -1
		}
		r.mu.Lock()
		defer r.mu.Unlock()
		if r.closed {
			return -1
		}
		id := r.next
		r.next++
		dctx, cancel := context.WithCancel(context.Background())
		conn := &rconn{cancel: cancel}
		r.conns[id] = conn
		go r.dial(dctx, conn, string(addr))
		return int64(id)
	}).Export("arti_host_tcp_connect").
		NewFunctionBuilder().WithFunc(func(_ context.Context, id uint32) int32 {
		conn := r.get(id)
		if conn == nil {
			return -1
		}
		conn.mu.Lock()
		defer conn.mu.Unlock()
		switch {
		case conn.nc != nil:
			return 1
		case conn.failed:
			return -1
		}
		return 0
	}).Export("arti_host_tcp_connect_status").
		NewFunctionBuilder().WithFunc(func(_ context.Context, mod api.Module, id, ptr, length uint32) int64 {
		conn := r.get(id)
		if conn == nil {
			return -3
		}
		conn.mu.Lock()
		defer conn.mu.Unlock()
		if len(conn.buf) > 0 {
			n := len(conn.buf)
			if uint32(n) > length {
				n = int(length)
			}
			mod.Memory().Write(ptr, conn.buf[:n])
			conn.buf = conn.buf[n:]
			return int64(n)
		}
		if conn.failed {
			return -3
		}
		if conn.eof {
			return -2
		}
		return -1
	}).Export("arti_host_tcp_read").
		NewFunctionBuilder().WithFunc(func(_ context.Context, mod api.Module, id, ptr, length uint32) int64 {
		conn := r.get(id)
		if conn == nil {
			return -3
		}
		src, ok := mod.Memory().Read(ptr, length)
		if !ok {
			return -3
		}
		nc := conn.conn()
		if nc == nil {
			return -3
		}
		data := make([]byte, len(src))
		copy(data, src)
		n, err := nc.Write(data)
		if err != nil {
			return -3
		}
		return int64(n)
	}).Export("arti_host_tcp_write").
		NewFunctionBuilder().WithFunc(func(_ context.Context, _ api.Module, id uint32) {
		r.mu.Lock()
		conn := r.conns[id]
		delete(r.conns, id)
		r.mu.Unlock()
		if conn != nil {
			conn.close()
		}
	}).Export("arti_host_tcp_close").
		NewFunctionBuilder().WithFunc(func(ctx context.Context, mod api.Module, timeout int32) {
		var expired <-chan time.Time
		if timeout >= 0 {
			t := time.NewTimer(time.Duration(timeout) * time.Millisecond)
			defer t.Stop()
			expired = t.C
		}
		select {
		case <-r.ready:
		case <-ctx.Done():
		case <-expired:
		}
		if ctx.Err() != nil || r.isClosed() {
			mod.CloseWithExitCode(context.Background(), 1)
			panic(sys.NewExitError(1))
		}
	}).Export("arti_host_poll").
		Instantiate(ctx)
	return err
}
