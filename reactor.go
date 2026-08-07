package arti

import (
	"context"
	"io"
	"net"
	"sync"
	"time"

	"github.com/tetratelabs/wazero/api"
)

// reactor implements the arti_host TCP host functions over real sockets and
// per-conn reader goroutines: connect/write block, reads are non-blocking
// (EAGAIN), and poll blocks until some connection becomes readable.
type reactor struct {
	mu     sync.Mutex
	conns  map[uint32]*rconn
	next   uint32
	ready  chan struct{}
	closed bool
}

// Close tears down every connection so a blocked read/poll returns at once,
// releasing the module's call lock for shutdown.
func (r *reactor) Close() {
	r.mu.Lock()
	r.closed = true
	for _, c := range r.conns {
		c.nc.Close()
	}
	r.mu.Unlock()
	r.signal()
}

type rconn struct {
	nc     net.Conn
	mu     sync.Mutex
	buf    []byte
	eof    bool
	failed bool
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

func (r *reactor) get(id uint32) *rconn {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.conns[id]
}

// pump reads from the socket into the conn's buffer, signalling the reactor so a
// blocked poll wakes up.
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

// registerHost defines the arti_host module the wasm imports.
func (c *Client) registerHost(ctx context.Context) error {
	r := c.reactor
	_, err := c.rt.NewHostModuleBuilder("arti_host").
		NewFunctionBuilder().WithFunc(func(_ context.Context, mod api.Module, ptr, length uint32) int64 {
		addr, ok := mod.Memory().Read(ptr, length)
		if !ok {
			return -1
		}
		nc, err := net.DialTimeout("tcp", string(addr), 30*time.Second)
		if err != nil {
			return -1
		}
		r.mu.Lock()
		if r.closed {
			r.mu.Unlock()
			nc.Close()
			return -1
		}
		id := r.next
		r.next++
		conn := &rconn{nc: nc}
		r.conns[id] = conn
		r.mu.Unlock()
		go r.pump(conn)
		return int64(id)
	}).Export("arti_host_tcp_connect").
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
		return -1 // EAGAIN
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
		data := make([]byte, len(src))
		copy(data, src)
		n, err := conn.nc.Write(data)
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
			conn.nc.Close()
		}
	}).Export("arti_host_tcp_close").
		NewFunctionBuilder().WithFunc(func(_ context.Context, _ api.Module, timeout int32) {
		if timeout < 0 {
			<-r.ready
			return
		}
		select {
		case <-r.ready:
		case <-time.After(time.Duration(timeout) * time.Millisecond):
		}
	}).Export("arti_host_poll").
		Instantiate(ctx)
	return err
}
