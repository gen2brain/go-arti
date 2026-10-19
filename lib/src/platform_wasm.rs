use std::cell::RefCell;
use std::io;
use std::net::SocketAddr;
use std::pin::Pin;
use std::task::{Context, Poll, Waker};
use std::time::Duration;

use futures::io::{AsyncRead, AsyncWrite};
use tor_rtcompat::StreamOps;

#[link(wasm_import_module = "arti_host")]
unsafe extern "C" {
    fn arti_host_tcp_connect(addr_ptr: *const u8, addr_len: usize) -> i64;
    fn arti_host_tcp_connect_status(id: u32) -> i32;
    fn arti_host_tcp_read(id: u32, ptr: *mut u8, len: usize) -> i64;
    fn arti_host_tcp_write(id: u32, ptr: *const u8, len: usize) -> i64;
    fn arti_host_tcp_close(id: u32);
    fn arti_host_poll(timeout_ms: i32);
}

thread_local! {
    static READ_WAKERS: RefCell<Vec<Waker>> = RefCell::new(Vec::new());
}

fn arm_read(waker: Waker) {
    READ_WAKERS.with(|w| w.borrow_mut().push(waker));
}

pub fn poll(timeout: Option<Duration>) {
    let ms = timeout
        .map(|d| d.as_millis().min(i32::MAX as u128) as i32)
        .unwrap_or(-1);
    unsafe { arti_host_poll(ms) };
    let wakers = READ_WAKERS.with(|w| std::mem::take(&mut *w.borrow_mut()));
    for w in wakers {
        w.wake();
    }
}

pub struct Conn {
    id: u32,
}

impl Conn {
    pub async fn connect(addr: SocketAddr) -> io::Result<Conn> {
        Self::connect_str(&addr.to_string()).await
    }

    pub async fn connect_str(addr: &str) -> io::Result<Conn> {
        let id = unsafe { arti_host_tcp_connect(addr.as_ptr(), addr.len()) };
        if id < 0 {
            return Err(io::Error::new(
                io::ErrorKind::ConnectionRefused,
                "host connect failed",
            ));
        }
        let conn = Conn { id: id as u32 };
        futures::future::poll_fn(
            |cx| match unsafe { arti_host_tcp_connect_status(conn.id) } {
                0 => {
                    arm_read(cx.waker().clone());
                    Poll::Pending
                }
                1 => Poll::Ready(Ok(())),
                _ => Poll::Ready(Err(io::Error::new(
                    io::ErrorKind::ConnectionRefused,
                    "host connect failed",
                ))),
            },
        )
        .await?;
        Ok(conn)
    }
}

impl Drop for Conn {
    fn drop(&mut self) {
        unsafe { arti_host_tcp_close(self.id) };
    }
}

impl AsyncRead for Conn {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        let n = unsafe { arti_host_tcp_read(self.id, buf.as_mut_ptr(), buf.len()) };
        match n {
            -1 => {
                arm_read(cx.waker().clone());
                Poll::Pending
            }
            -2 => Poll::Ready(Ok(0)),
            x if x < 0 => Poll::Ready(Err(io::Error::other("host read error"))),
            n => Poll::Ready(Ok(n as usize)),
        }
    }
}

impl AsyncWrite for Conn {
    fn poll_write(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let n = unsafe { arti_host_tcp_write(self.id, buf.as_ptr(), buf.len()) };
        if n < 0 {
            return Poll::Ready(Err(io::Error::other("host write error")));
        }
        Poll::Ready(Ok(n as usize))
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_close(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

impl StreamOps for Conn {}
