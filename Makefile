clean: clean-nodejs clean-rust
	rm -rf test-ledger

clean-nodejs:
	rm -rf examples/typescript/dist
	rm -rf examples/typescript/node_modules
	rm -rf yellowstone-grpc-client-nodejs/dist
	rm -rf yellowstone-grpc-client-nodejs/node_modules
	rm -rf yellowstone-grpc-client-nodejs/src/encoding
	rm -rf yellowstone-grpc-client-nodejs/src/grpc

clean-rust:
	rm -rf target
	rm -rf yellowstone-grpc-client-nodejs/solana-encoding-wasm/target

solana-encoding-wasm-install-dependencies:
	rustup target add wasm32-unknown-unknown
	cargo install -f wasm-bindgen-cli --version 0.2.100

solana-encoding-wasm-clippy:
	cd yellowstone-grpc-client-nodejs/solana-encoding-wasm && \
		RUSTFLAGS='--cfg getrandom_backend="wasm_js"' cargo clippy --target wasm32-unknown-unknown --all-targets

solana-encoding-wasm-build:
	# RUSTFLAGS to disable `mold`
	cd yellowstone-grpc-client-nodejs/solana-encoding-wasm && \
		RUSTFLAGS='--cfg getrandom_backend="wasm_js"' cargo build \
			--target wasm32-unknown-unknown \
			--release

	cd yellowstone-grpc-client-nodejs/solana-encoding-wasm && \
		rm -rf ../src/encoding/ && \
		wasm-bindgen \
			--target nodejs \
			--out-dir ../src/encoding/ \
			target/wasm32-unknown-unknown/release/yellowstone_grpc_solana_encoding_wasm.wasm && \
		mkdir -p ../dist/encoding/ && \
		cp -ap ../src/encoding/ ../dist/
