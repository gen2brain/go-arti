## go-arti

Self-contained, CGo-free Tor client for Go.

It embeds the Rust [arti-client](https://docs.rs/arti-client) compiled to [WASM](https://en.wikipedia.org/wiki/WebAssembly) (`wasm32-wasip1`) and runs it
with [wazero](https://github.com/tetratelabs/wazero), so there is no CGo, no external `tor` daemon, and no system dependency.

### Usage

```go
c, err := arti.New(context.Background(), "") // "" = temporary dir cache
if err != nil {
	log.Fatal(err)
}
defer c.Close()

if err := c.Bootstrap(); err != nil { // connect to Tor (tens of seconds)
	log.Fatal(err)
}

// net.Conn over Tor (clearnet or .onion):
conn, err := c.Dial("tcp", "example.com:80")

// http.Client (https TLS is done by Go over the Tor stream):
resp, err := c.HTTPClient().Get("https://example.com/")

// host an onion service, reverse-proxied to a local address (its key persists
// if a cache dir is given to New):
svc, err := c.ServeOnion("myservice", "127.0.0.1:8080")
addr := svc.Address() // "xxxx....onion"
```

A `Client` is one single-threaded wasm instance, so calls into it are serialized (`HTTPClient` therefore uses a single-request-per-connection round tripper, not `http.Transport`).
For concurrency, use a `Pool` of independent instances, which exposes the same Dialer / `HTTPClient` API:

```go
p, err := arti.NewPool(context.Background(), "", 4) // 4 independent instances
```

By default the connections of one instance may share a Tor circuit, which limits parallel streams to the bandwidth of that circuit.
`WithCircuits` spreads them over isolated circuits instead, for a `Client` or every instance of a `Pool`:

```go
p, err := arti.NewPool(context.Background(), "", 2, arti.WithCircuits(2)) // 2 instances, 4 circuit groups
```

See `examples/fetch` for a runnable demo. `go test -short` runs the unit tests;
`go test` also runs slow tests that bootstrap the real Tor network.

### Building the wasm

The `lib/arti.wasm.gz` is embedded, so consumers need only Go.
To rebuild it after changing the Rust in `lib/`, you need the `wasm32-wasip1` Rust target, [wasi-sdk](https://github.com/WebAssembly/wasi-sdk) (for the bundled sqlite),
and `wasm-opt` from [binaryen](https://github.com/WebAssembly/binaryen):

```
rustup target add wasm32-wasip1
make -C lib   # WASI_SDK=/opt/wasi-sdk, WASM_OPT=/opt/emsdk/upstream/bin/wasm-opt
```
