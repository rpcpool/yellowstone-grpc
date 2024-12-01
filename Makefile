clean: clean-nodejs clean-rust

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

solana-encoding-wasm-clippy:
	cd yellowstone-grpc-client-nodejs/solana-encoding-wasm && \
		cargo clippy --target wasm32-unknown-unknown --all-targets

solana-encoding-wasm-build:
	# RUSTFLAGS to disable `mold`
	cd yellowstone-grpc-client-nodejs/solana-encoding-wasm && \
		RUSTFLAGS="" cargo build \
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
