clean: clean-nodejs clean-rust clean-napi
	rm -rf test-ledger

clean-nodejs:
	rm -rf examples/typescript/dist
	rm -rf examples/typescript/node_modules
	rm -rf yellowstone-grpc-client-nodejs/dist
	rm -rf yellowstone-grpc-client-nodejs/node_modules
	rm -rf yellowstone-grpc-client-nodejs/src/encoding
	rm -rf yellowstone-grpc-client-nodejs/src/grpc
	rm -rf yellowstone-grpc-client-nodejs/src/napi

clean-rust:
	rm -rf target
	rm -rf yellowstone-grpc-client-nodejs/napi/target

clean-napi:
	rm -rf yellowstone-grpc-client-nodejs/napi/target
	rm -rf yellowstone-grpc-client-nodejs/napi/*.node
	rm -rf yellowstone-grpc-client-nodejs/napi/index.js
	rm -rf yellowstone-grpc-client-nodejs/napi/index.d.ts

solana-encoding-napi-clippy:
	cd yellowstone-grpc-client-nodejs/napi && \
		cargo clippy
