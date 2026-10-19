use std::error::Error;

use arti_client::{TorClient, TorClientConfig};
use futures::io::{AsyncReadExt, AsyncWriteExt};

fn main() {
    let rt = arti_shim::arti_runtime();
    let code = match arti_shim::block_on(run(rt)) {
        Ok(()) => {
            println!("BOOTSTRAP TEST OK");
            0
        }
        Err(e) => {
            println!("BOOTSTRAP TEST FAILED: {e}");
            1
        }
    };
    arti_shim::exec::forget_pending();
    std::process::exit(code);
}

async fn run(rt: arti_shim::ArtiRuntime) -> Result<(), Box<dyn Error>> {
    let config = TorClientConfig::default();

    println!("[boot] bootstrapping Tor (this downloads a consensus; give it a minute)...");
    let client = TorClient::with_runtime(rt)
        .config(config)
        .create_bootstrapped()
        .await?;
    println!("[boot] bootstrapped!");

    println!("[boot] opening a Tor stream to example.com:80 ...");
    let mut stream = client.connect(("example.com", 80)).await?;
    println!("[boot] stream open; sending request");

    stream
        .write_all(b"GET / HTTP/1.0\r\nHost: example.com\r\nConnection: close\r\n\r\n")
        .await?;
    stream.flush().await?;

    let mut buf = vec![0u8; 1024];
    let n = stream.read(&mut buf).await?;
    let text = String::from_utf8_lossy(&buf[..n]);
    let status = text.lines().next().unwrap_or("");
    println!("[boot] got {n} bytes over Tor; status: {status}");

    Ok(())
}
