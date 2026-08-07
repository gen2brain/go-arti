//! arti-shim: a custom single-threaded `tor-rtcompat` runtime for Arti that runs
//! natively (for testing) and on `wasm32-wasip1` under wazero. The wasm build
//! uses WASI for the filesystem, clock and randomness; only TCP is provided by
//! the host (the `arti_host` import module). The wasm C ABI consumed by the Go
//! package lives in `capi.rs`.

mod capi;
pub mod exec;
pub mod net;
pub mod runtime;
pub mod tls;

#[cfg(not(target_arch = "wasm32"))]
#[path = "platform_native.rs"]
mod platform;

#[cfg(target_arch = "wasm32")]
#[path = "platform_wasm.rs"]
mod platform;

pub use exec::block_on;
pub use runtime::{arti_runtime, ArtiRuntime};
