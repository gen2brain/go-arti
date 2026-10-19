use std::io;
use std::time::Duration;

use async_trait::async_trait;
use futures::task::{FutureObj, Spawn, SpawnError};

use tor_rtcompat::{
    Blocking, CompoundRuntime, RealCoarseTimeProvider, SleepProvider, ToplevelBlockOn, UdpProvider,
    UdpSocket,
};

use crate::net::TcpProvider;
use crate::tls::TorTls;

#[derive(Clone, Debug)]
pub struct Exec;

impl Spawn for Exec {
    fn spawn_obj(&self, future: FutureObj<'static, ()>) -> Result<(), SpawnError> {
        crate::exec::spawn(future);
        Ok(())
    }
}

impl ToplevelBlockOn for Exec {
    fn block_on<F: std::future::Future>(&self, f: F) -> F::Output {
        crate::exec::block_on(f)
    }
}

impl Blocking for Exec {
    type ThreadHandle<T: Send + 'static> = futures::future::Ready<T>;

    fn spawn_blocking<F, T>(&self, f: F) -> Self::ThreadHandle<T>
    where
        F: FnOnce() -> T + Send + 'static,
        T: Send + 'static,
    {
        futures::future::ready(f())
    }

    fn reenter_block_on<F>(&self, future: F) -> F::Output
    where
        F: std::future::Future,
        F::Output: Send + 'static,
    {
        crate::exec::block_on(future)
    }
}

#[derive(Clone, Debug)]
pub struct TimeP;

impl SleepProvider for TimeP {
    type SleepFuture = crate::exec::Sleep;
    fn sleep(&self, duration: Duration) -> Self::SleepFuture {
        crate::exec::sleep(duration)
    }
}

#[derive(Clone, Debug)]
pub struct NoUdp;

#[async_trait]
impl UdpProvider for NoUdp {
    type UdpSocket = NoUdpSocket;
    async fn bind(&self, _addr: &std::net::SocketAddr) -> io::Result<NoUdpSocket> {
        Err(io::Error::new(io::ErrorKind::Unsupported, "udp not supported"))
    }
}

pub enum NoUdpSocket {}

#[async_trait]
impl UdpSocket for NoUdpSocket {
    async fn recv(&self, _buf: &mut [u8]) -> io::Result<(usize, std::net::SocketAddr)> {
        match *self {}
    }
    async fn send(&self, _buf: &[u8], _target: &std::net::SocketAddr) -> io::Result<usize> {
        match *self {}
    }
    fn local_addr(&self) -> io::Result<std::net::SocketAddr> {
        match *self {}
    }
}

pub type ArtiRuntime =
    CompoundRuntime<Exec, TimeP, RealCoarseTimeProvider, TcpProvider, TcpProvider, TorTls, NoUdp>;

pub fn arti_runtime() -> ArtiRuntime {
    CompoundRuntime::new(
        Exec,
        TimeP,
        RealCoarseTimeProvider::new(),
        TcpProvider,
        TcpProvider,
        TorTls::new(),
        NoUdp,
    )
}
