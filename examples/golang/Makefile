protoc:
	protoc \
		--go_out=./proto \
		--go_opt=paths=source_relative \
		--go-grpc_out=./proto \
		--go-grpc_opt=paths=source_relative \
		--experimental_allow_proto3_optional \
		--proto_path ../../yellowstone-grpc-proto/proto/ ../../yellowstone-grpc-proto/proto/*.proto
