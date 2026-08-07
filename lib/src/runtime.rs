//! Assembly of the custom `CompoundRuntime` from our providers.

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

// --- executor provider -----------------------------------------------------

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
        // No threads: run inline. On wasm there is no real blocking IO - network
        // is async via the host, and sqlite/fs go through WASI.
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

// --- time provider ---------------------------------------------------------

#[derive(Clone, Debug)]
pub struct TimeP;

impl SleepProvider for TimeP {
    type SleepFuture = crate::exec::Sleep;
    fn sleep(&self, duration: Duration) -> Self::SleepFuture {
        crate::exec::sleep(duration)
    }
    // now() / wallclock() use the defaults, backed by web-time (host-backed on wasm).
}

// --- udp provider (unsupported) --------------------------------------------

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

// --- assembly --------------------------------------------------------------

/// The concrete runtime type Arti runs on.
pub type ArtiRuntime =
    CompoundRuntime<Exec, TimeP, RealCoarseTimeProvider, TcpProvider, TcpProvider, TorTls, NoUdp>;

/// Construct the runtime.
pub fn arti_runtime() -> ArtiRuntime {
    CompoundRuntime::new(
        Exec,
        TimeP,
        RealCoarseTimeProvider::new(),
        TcpProvider, // tcp slot
        TcpProvider, // unix slot (unsupported stub)
        TorTls::new(),
        NoUdp,
    )
}
