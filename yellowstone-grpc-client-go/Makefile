PROTO_SRC := ../yellowstone-grpc-proto/proto
GO_PKG    := github.com/rpcpool/yellowstone-grpc/yellowstone-grpc-client-go/proto

protoc:
	protoc \
		--go_out=./proto \
		--go_opt=paths=source_relative \
		--go_opt=Mgeyser.proto=$(GO_PKG) \
		--go_opt=Msolana-storage.proto=$(GO_PKG) \
		--go-grpc_out=./proto \
		--go-grpc_opt=paths=source_relative \
		--go-grpc_opt=Mgeyser.proto=$(GO_PKG) \
		--go-grpc_opt=Msolana-storage.proto=$(GO_PKG) \
		--experimental_allow_proto3_optional \
		--proto_path $(PROTO_SRC) $(PROTO_SRC)/*.proto

install-protoc:
	go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
	go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
	@echo 'ensure $$(go env GOPATH)/bin is on PATH'

build:
	go build ./...

test:
	go test ./...

tidy:
	go mod tidy

.PHONY: protoc install-protoc build test tidy
