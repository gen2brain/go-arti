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
