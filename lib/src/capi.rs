#![cfg(target_arch = "wasm32")]

use std::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::future::Future;
use std::time::Duration;

use arti_client::config::CfgPath;
use std::sync::Arc;

use arti_client::{DataStream, IsolationToken, StreamPrefs, TorClient, TorClientConfig};
use futures::future::FutureExt;
use futures::io::{AsyncReadExt, AsyncWriteExt};

use crate::{arti_runtime, block_on, ArtiRuntime};

const TIMEOUT: i64 = -4;

async fn with_timeout<F: Future>(timeout_ms: i32, fut: F) -> Option<F::Output> {
    if timeout_ms <= 0 {
        return Some(fut.await);
    }
    let timeout = crate::exec::sleep(Duration::from_millis(timeout_ms as u64));
    futures::pin_mut!(fut, timeout);
    futures::select! {
        v = fut.fuse() => Some(v),
        _ = timeout.fuse() => None,
    }
}

enum Pending {
    Connecting,
    Ready(DataStream),
    Failed(String),
}

thread_local! {
    static CLIENT: RefCell<Option<Arc<TorClient<ArtiRuntime>>>> = const { RefCell::new(None) };
    static GROUPS: RefCell<HashMap<u32, IsolationToken>> = RefCell::new(HashMap::new());
    static STREAMS: RefCell<HashMap<u32, DataStream>> = RefCell::new(HashMap::new());
    static PENDING: RefCell<HashMap<u32, Pending>> = RefCell::new(HashMap::new());
    static NEXT_ID: Cell<u32> = const { Cell::new(1) };
    static LAST_ERR: RefCell<String> = const { RefCell::new(String::new()) };
}

fn set_err(msg: impl Into<String>) {
    let msg = msg.into();
    eprintln!("go-arti: {msg}");
    LAST_ERR.with(|e| *e.borrow_mut() = msg);
}

fn record_err(msg: impl Into<String>) {
    LAST_ERR.with(|e| *e.borrow_mut() = msg.into());
}

#[unsafe(no_mangle)]
pub extern "C" fn arti_last_error(out_ptr: *mut u8, out_cap: usize) -> i64 {
    LAST_ERR.with(|e| {
        let bytes = e.borrow();
        let bytes = bytes.as_bytes();
        let n = bytes.len().min(out_cap);
        unsafe { std::slice::from_raw_parts_mut(out_ptr, n).copy_from_slice(&bytes[..n]) };
        n as i64
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn arti_alloc(len: usize) -> *mut u8 {
    let mut buf = Vec::<u8>::with_capacity(len);
    let ptr = buf.as_mut_ptr();
    std::mem::forget(buf);
    ptr
}

#[unsafe(no_mangle)]
pub extern "C" fn arti_free(ptr: *mut u8, len: usize) {
    if !ptr.is_null() {
        unsafe { drop(Vec::from_raw_parts(ptr, 0, len)) };
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn arti_bootstrap() -> i32 {
    std::panic::set_hook(Box::new(|info| eprintln!("go-arti panic: {info}")));
    let _ = tracing_subscriber::fmt()
        .with_ansi(false)
        .without_time()
        .with_max_level(tracing::Level::WARN)
        .try_init();

    if CLIENT.with(|c| c.borrow().is_some()) {
        return 0;
    }

    let result: Result<Arc<TorClient<ArtiRuntime>>, Box<dyn std::error::Error>> = block_on(async {
        let mut builder = TorClientConfig::builder();
        builder
            .storage()
            .cache_dir(CfgPath::new_literal("/arti/cache"))
            .state_dir(CfgPath::new_literal("/arti/state"));
        let config = builder.build()?;
        let client = TorClient::with_runtime(arti_runtime())
            .config(config)
            .create_bootstrapped()
            .await?;
        Ok(client)
    });

    match result {
        Ok(client) => {
            CLIENT.with(|c| *c.borrow_mut() = Some(client));
            0
        }
        Err(e) => {
            set_err(format!("bootstrap failed: {e}"));
            -1
        }
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn arti_connect_start(host_ptr: *const u8, host_len: usize, port: u16, group: u32) -> i64 {
    let host = match std::str::from_utf8(unsafe {
        std::slice::from_raw_parts(host_ptr, host_len)
    }) {
        Ok(h) => h.to_string(),
        Err(_) => {
            set_err("connect: host is not valid UTF-8");
            return -1;
        }
    };

    let client = CLIENT.with(|c| c.borrow().clone());
    let Some(client) = client else {
        set_err("connect: client not bootstrapped");
        return -1;
    };

    let id = NEXT_ID.with(|n| {
        let v = n.get();
        n.set(v + 1);
        v
    });
    PENDING.with(|p| p.borrow_mut().insert(id, Pending::Connecting));

    let isolation = (group != 0)
        .then(|| GROUPS.with(|g| *g.borrow_mut().entry(group).or_insert_with(IsolationToken::new)));

    crate::exec::spawn(async move {
        let result = match isolation {
            Some(token) => {
                let mut prefs = StreamPrefs::new();
                prefs.set_isolation(token);
                client.connect_with_prefs((host.clone(), port), &prefs).await
            }
            None => client.connect((host.clone(), port)).await,
        };
        let slot = match result {
            Ok(stream) => Pending::Ready(stream),
            Err(e) => Pending::Failed(format!("connect to {host}:{port} failed: {e}")),
        };
        PENDING.with(|p| {
            if let Some(entry) = p.borrow_mut().get_mut(&id) {
                *entry = slot;
            }
        });
    });

    id as i64
}

#[unsafe(no_mangle)]
pub extern "C" fn arti_connect_poll(id: u32) -> i32 {
    let state = PENDING.with(|p| match p.borrow().get(&id) {
        Some(Pending::Connecting) => 0,
        Some(Pending::Ready(_)) => 1,
        Some(Pending::Failed(_)) => -1,
        None => -2,
    });
    match state {
        1 => {
            if let Some(Pending::Ready(stream)) = PENDING.with(|p| p.borrow_mut().remove(&id)) {
                STREAMS.with(|s| s.borrow_mut().insert(id, stream));
            }
            1
        }
        -1 => {
            if let Some(Pending::Failed(msg)) = PENDING.with(|p| p.borrow_mut().remove(&id)) {
                record_err(msg);
            }
            -1
        }
        -2 => {
            record_err("connect: unknown stream id");
            -1
        }
        _ => 0,
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn arti_connect_cancel(id: u32) {
    PENDING.with(|p| {
        p.borrow_mut().remove(&id);
    });
    STREAMS.with(|s| {
        s.borrow_mut().remove(&id);
    });
}

#[unsafe(no_mangle)]
pub extern "C" fn arti_read(id: u32, ptr: *mut u8, len: usize, timeout_ms: i32) -> i64 {
    STREAMS.with(|s| {
        let mut streams = s.borrow_mut();
        let Some(stream) = streams.get_mut(&id) else {
            record_err(format!("read: no stream {id}"));
            return -1;
        };
        let buf = unsafe { std::slice::from_raw_parts_mut(ptr, len) };
        match block_on(with_timeout(timeout_ms, stream.read(buf))) {
            Some(Ok(n)) => n as i64,
            Some(Err(e)) => {
                record_err(format!("read on stream {id} failed: {e}"));
                -1
            }
            None => TIMEOUT,
        }
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn arti_write(id: u32, ptr: *const u8, len: usize, timeout_ms: i32) -> i64 {
    STREAMS.with(|s| {
        let mut streams = s.borrow_mut();
        let Some(stream) = streams.get_mut(&id) else {
            record_err(format!("write: no stream {id}"));
            return -1;
        };
        let buf = unsafe { std::slice::from_raw_parts(ptr, len) };
        let fut = async {
            stream.write_all(buf).await?;
            stream.flush().await?;
            Ok::<usize, std::io::Error>(buf.len())
        };
        match block_on(with_timeout(timeout_ms, fut)) {
            Some(Ok(n)) => n as i64,
            Some(Err(e)) => {
                record_err(format!("write on stream {id} failed: {e}"));
                -1
            }
            None => TIMEOUT,
        }
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn arti_close(id: u32) {
    let stream = STREAMS.with(|s| s.borrow_mut().remove(&id));
    if let Some(mut stream) = stream {
        let _ = block_on(stream.close());
    }
}

thread_local! {
    static SERVICES: RefCell<Vec<std::sync::Arc<tor_hsservice::RunningOnionService>>> =
        const { RefCell::new(Vec::new()) };
}

#[unsafe(no_mangle)]
pub extern "C" fn arti_onion_launch(
    nick_ptr: *const u8,
    nick_len: usize,
    target_ptr: *const u8,
    target_len: usize,
    out_ptr: *mut u8,
    out_cap: usize,
) -> i64 {
    use safelog::DisplayRedacted;
    use tor_hsservice::config::OnionServiceConfigBuilder;
    use tor_hsservice::HsNickname;

    let nick = match std::str::from_utf8(unsafe { std::slice::from_raw_parts(nick_ptr, nick_len) })
    {
        Ok(n) => n,
        Err(_) => {
            set_err("onion launch: nickname is not valid UTF-8");
            return -1;
        }
    };
    let target =
        match std::str::from_utf8(unsafe { std::slice::from_raw_parts(target_ptr, target_len) }) {
            Ok(t) => t.to_owned(),
            Err(_) => {
                set_err("onion launch: target is not valid UTF-8");
                return -1;
            }
        };
    let nickname: HsNickname = match nick.parse() {
        Ok(n) => n,
        Err(e) => {
            set_err(format!("onion launch: invalid nickname {nick:?}: {e}"));
            return -1;
        }
    };
    let config = match OnionServiceConfigBuilder::default().nickname(nickname).build() {
        Ok(c) => c,
        Err(e) => {
            set_err(format!("onion launch: config build failed: {e}"));
            return -1;
        }
    };

    CLIENT.with(|c| {
        let client = c.borrow();
        let Some(client) = client.as_ref() else {
            set_err("onion launch: client not bootstrapped");
            return -1;
        };
        let (running, requests) = match client.launch_onion_service(config) {
            Ok(Some(v)) => v,
            Ok(None) => {
                set_err("onion launch: no service returned");
                return -1;
            }
            Err(e) => {
                set_err(format!("onion launch failed: {e}"));
                return -1;
            }
        };
        let addr = match running.onion_address() {
            Some(a) => a.display_unredacted().to_string(),
            None => {
                set_err("onion launch: service has no address yet");
                return -1;
            }
        };
        SERVICES.with(|s| s.borrow_mut().push(running));
        crate::exec::spawn(serve_onion(requests, target));

        let bytes = addr.as_bytes();
        let n = bytes.len().min(out_cap);
        unsafe { std::slice::from_raw_parts_mut(out_ptr, n).copy_from_slice(&bytes[..n]) };
        n as i64
    })
}

async fn serve_onion<S>(requests: S, target: String)
where
    S: futures::Stream<Item = tor_hsservice::RendRequest>,
{
    use futures::StreamExt;

    let streams = tor_hsservice::handle_rend_requests(requests);
    futures::pin_mut!(streams);
    while let Some(req) = streams.next().await {
        let target = target.clone();
        crate::exec::spawn(proxy_stream(req, target));
    }
}

async fn proxy_stream(req: tor_hsservice::StreamRequest, target: String) {
    use tor_cell::relaycell::msg::Connected;

    let onion = match req.accept(Connected::new_empty()).await {
        Ok(s) => s,
        Err(e) => {
            set_err(format!("onion accept failed: {e}"));
            return;
        }
    };
    let local = match crate::platform::Conn::connect_str(&target).await {
        Ok(c) => c,
        Err(e) => {
            set_err(format!("onion proxy: dial {target} failed: {e}"));
            return;
        }
    };

    let (onion_r, onion_w) = onion.split();
    let (local_r, local_w) = local.split();
    let up = pipe(onion_r, local_w);
    let down = pipe(local_r, onion_w);
    futures::pin_mut!(up, down);
    futures::future::select(up, down).await;
}

async fn pipe<R, W>(mut r: R, mut w: W)
where
    R: futures::io::AsyncRead + Unpin,
    W: futures::io::AsyncWrite + Unpin,
{
    let mut buf = [0u8; 8192];
    loop {
        match r.read(&mut buf).await {
            Ok(0) | Err(_) => break,
            Ok(n) => {
                if w.write_all(&buf[..n]).await.is_err() || w.flush().await.is_err() {
                    break;
                }
            }
        }
    }
    let _ = w.close().await;
}

#[unsafe(no_mangle)]
pub extern "C" fn arti_pump(budget_ms: i32) {
    let ms = if budget_ms < 0 { 0 } else { budget_ms as u64 };
    crate::exec::pump(Duration::from_millis(ms));
}
