use std::cell::RefCell;
use std::io::{self, Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::os::fd::{AsFd, AsRawFd, BorrowedFd};
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::task::{Context, Poll, Waker};
use std::time::Duration;

use futures::io::{AsyncRead, AsyncWrite};
use polling::{Event, Events, Poller};
use socket2::{Domain, Protocol, Socket, Type};
use tor_rtcompat::StreamOps;

struct Source {
    read: Option<Waker>,
    write: Option<Waker>,
    interest_read: bool,
    interest_write: bool,
    fd: i32,
}

struct Reactor {
    poller: Poller,
    sources: Vec<Option<Source>>,
    events: Events,
}

thread_local! {
    static REACTOR: RefCell<Reactor> = RefCell::new(Reactor {
        poller: Poller::new().expect("create poller"),
        sources: Vec::new(),
        events: Events::new(),
    });
}

static NEXT_KEY: AtomicU64 = AtomicU64::new(1);

struct FdRef(i32);
impl AsFd for FdRef {
    fn as_fd(&self) -> BorrowedFd<'_> {
        // Safety: the fd is owned by a live socket for the source's lifetime.
        unsafe { BorrowedFd::borrow_raw(self.0) }
    }
}
impl AsRawFd for FdRef {
    fn as_raw_fd(&self) -> i32 {
        self.0
    }
}

pub fn register(fd: i32) -> usize {
    REACTOR.with(|r| {
        let mut r = r.borrow_mut();
        let key = NEXT_KEY.fetch_add(1, Ordering::Relaxed) as usize;
        if r.sources.len() <= key {
            r.sources.resize_with(key + 1, || None);
        }
        r.sources[key] = Some(Source {
            read: None,
            write: None,
            interest_read: false,
            interest_write: false,
            fd,
        });
        unsafe {
            r.poller.add(&FdRef(fd), Event::none(key)).expect("poller add");
        }
        key
    })
}

fn rearm(r: &mut Reactor, key: usize) {
    if let Some(Some(s)) = r.sources.get(key) {
        let mut ev = Event::none(key);
        ev.readable = s.interest_read;
        ev.writable = s.interest_write;
        r.poller.modify(&FdRef(s.fd), ev).ok();
    }
}

pub fn arm_read(key: usize, waker: Waker) {
    REACTOR.with(|r| {
        let mut r = r.borrow_mut();
        if let Some(Some(s)) = r.sources.get_mut(key) {
            s.read = Some(waker);
            s.interest_read = true;
        }
        rearm(&mut r, key);
    });
}

pub fn arm_write(key: usize, waker: Waker) {
    REACTOR.with(|r| {
        let mut r = r.borrow_mut();
        if let Some(Some(s)) = r.sources.get_mut(key) {
            s.write = Some(waker);
            s.interest_write = true;
        }
        rearm(&mut r, key);
    });
}

pub fn deregister(key: usize) {
    REACTOR.with(|r| {
        let mut r = r.borrow_mut();
        if let Some(slot) = r.sources.get_mut(key) {
            if let Some(s) = slot.take() {
                r.poller.delete(&FdRef(s.fd)).ok();
            }
        }
    });
}

pub fn poll(timeout: Option<Duration>) {
    REACTOR.with(|r| {
        let mut r = r.borrow_mut();
        let mut events = std::mem::take(&mut r.events);
        events.clear();
        r.poller.wait(&mut events, timeout).ok();
        for ev in events.iter() {
            let key = ev.key;
            if let Some(Some(s)) = r.sources.get_mut(key) {
                if ev.readable {
                    s.interest_read = false;
                    if let Some(w) = s.read.take() {
                        w.wake();
                    }
                }
                if ev.writable {
                    s.interest_write = false;
                    if let Some(w) = s.write.take() {
                        w.wake();
                    }
                }
            }
        }
        r.events = events;
    });
}

pub struct Conn {
    stream: TcpStream,
    key: usize,
}

impl Conn {
    pub async fn connect(addr: SocketAddr) -> io::Result<Conn> {
        let sock = Socket::new(Domain::for_address(addr), Type::STREAM, Some(Protocol::TCP))?;
        sock.set_nonblocking(true)?;
        match sock.connect(&addr.into()) {
            Ok(()) => {}
            Err(e) if e.raw_os_error() == Some(115) => {}
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => {}
            Err(e) => return Err(e),
        }
        let stream: TcpStream = sock.into();
        let key = register(stream.as_raw_fd());
        let c = Conn { stream, key };

        WaitWritable { key, armed: false }.await;
        if let Some(err) = c.stream.take_error()? {
            return Err(err);
        }
        Ok(c)
    }
}

impl Drop for Conn {
    fn drop(&mut self) {
        deregister(self.key);
    }
}

impl AsyncRead for Conn {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        match (&self.stream).read(buf) {
            Ok(n) => Poll::Ready(Ok(n)),
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                arm_read(self.key, cx.waker().clone());
                Poll::Pending
            }
            Err(e) => Poll::Ready(Err(e)),
        }
    }
}

impl AsyncWrite for Conn {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        match (&self.stream).write(buf) {
            Ok(n) => Poll::Ready(Ok(n)),
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                arm_write(self.key, cx.waker().clone());
                Poll::Pending
            }
            Err(e) => Poll::Ready(Err(e)),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready((&self.stream).flush())
    }

    fn poll_close(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let _ = self.stream.shutdown(std::net::Shutdown::Write);
        Poll::Ready(Ok(()))
    }
}

impl StreamOps for Conn {}

struct WaitWritable {
    key: usize,
    armed: bool,
}
impl std::future::Future for WaitWritable {
    type Output = ();
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        if self.armed {
            return Poll::Ready(());
        }
        self.armed = true;
        arm_write(self.key, cx.waker().clone());
        Poll::Pending
    }
}
