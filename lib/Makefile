WASI_SDK ?= /opt/wasi-sdk
WASM_OPT ?= /opt/emsdk/upstream/bin/wasm-opt
TARGET    = wasm32-wasip1
SHIM      = target/$(TARGET)/release/arti_shim.wasm
OPT       = target/$(TARGET)/release/arti_shim.opt.wasm
OUT       = arti.wasm.gz

PATCH_SPECS = arti-client:0.44.0 tor-dirmgr:0.44.0 tor-rtcompat:0.44.0 fslock-guard:0.8.0

WASI_ENV = \
	CC_wasm32_wasip1=$(WASI_SDK)/bin/clang \
	CFLAGS_wasm32_wasip1="--target=$(TARGET) --sysroot=$(WASI_SDK)/share/wasi-sysroot" \
	AR_wasm32_wasip1=$(WASI_SDK)/bin/llvm-ar

all: $(OUT)

patches/.applied:
	@for spec in $(PATCH_SPECS); do \
	  name=$${spec%%:*}; ver=$${spec##*:}; dir=patches/$$name; \
	  if [ ! -d $$dir ]; then \
	    echo "  patch $$name $$ver"; \
	    rm -rf patches/$$name-$$ver; \
	    curl -sSL "https://static.crates.io/crates/$$name/$$name-$$ver.crate" | tar -xz -C patches; \
	    mv patches/$$name-$$ver $$dir; \
	    patch -s -p1 -d $$dir < patches/$$name.patch; \
	  fi; \
	done
	@touch $@

$(SHIM): patches/.applied $(wildcard src/*.rs) $(wildcard src/bin/*.rs) Cargo.toml
	$(WASI_ENV) cargo build --release --lib --target $(TARGET)

$(OPT): $(SHIM)
	$(WASM_OPT) -O3 $(SHIM) -o $(OPT)

$(OUT): $(OPT)
	gzip -9 -c $(OPT) > $@

native: patches/.applied
	cargo run --release --bin bootstrap

.PHONY: all native clean
clean:
	cargo clean
	rm -rf $(foreach s,$(PATCH_SPECS),patches/$(firstword $(subst :, ,$(s)))) patches/.applied
