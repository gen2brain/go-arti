use std::io;
use std::marker::PhantomData;
use std::net::SocketAddr;
use std::pin::Pin;
use std::task::{Context, Poll};

use async_trait::async_trait;
use futures::io::{AsyncRead, AsyncWrite};
use tor_general_addr::unix;
use tor_rtcompat::{
    NetStreamListener, NetStreamProvider, StreamOps, TcpConnectOptions, TcpListenOptions,
    UnixListenOptions,
};

use crate::platform::Conn;

fn unsupported(what: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::Unsupported, what)
}

#[derive(Clone, Default)]
pub struct TcpProvider;

#[async_trait]
impl NetStreamProvider<SocketAddr> for TcpProvider {
    type Stream = Conn;
    type Listener = StubListener<Conn, SocketAddr>;
    type ConnectOptions = TcpConnectOptions;
    type ListenOptions = TcpListenOptions;

    async fn connect(&self, addr: &SocketAddr, _options: &TcpConnectOptions) -> io::Result<Conn> {
        Conn::connect(*addr).await
    }

    async fn listen(
        &self,
        _addr: &SocketAddr,
        _options: &TcpListenOptions,
    ) -> io::Result<Self::Listener> {
        Err(unsupported("tcp listen not yet supported"))
    }
}

#[async_trait]
impl NetStreamProvider<unix::SocketAddr> for TcpProvider {
    type Stream = Void;
    type Listener = StubListener<Void, unix::SocketAddr>;
    type ConnectOptions = ();
    type ListenOptions = UnixListenOptions;

    async fn connect(&self, _addr: &unix::SocketAddr, _options: &()) -> io::Result<Void> {
        Err(unsupported("unix sockets not supported"))
    }

    async fn listen(
        &self,
        _addr: &unix::SocketAddr,
        _options: &UnixListenOptions,
    ) -> io::Result<Self::Listener> {
        Err(unsupported("unix sockets not supported"))
    }
}

pub struct StubListener<S, A>(PhantomData<fn() -> (S, A)>);

impl<S, A> NetStreamListener<A> for StubListener<S, A>
where
    S: AsyncRead + AsyncWrite + StreamOps + Send + Sync + Unpin + 'static,
    A: Send + Sync + Unpin + 'static,
{
    type Stream = S;
    type Incoming = futures::stream::Pending<io::Result<(S, A)>>;

    fn incoming(self) -> Self::Incoming {
        futures::stream::pending()
    }

    fn local_addr(&self) -> io::Result<A> {
        Err(unsupported("no listener"))
    }
}

pub enum Void {}

impl AsyncRead for Void {
    fn poll_read(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        _buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        match *self {}
    }
}
impl AsyncWrite for Void {
    fn poll_write(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        _buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        match *self {}
    }
    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match *self {}
    }
    fn poll_close(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match *self {}
    }
}
impl StreamOps for Void {}
