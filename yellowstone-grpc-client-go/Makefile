PROTO_SRC := ../yellowstone-grpc-proto/proto
GO_PKG    := github.com/rpcpool/yellowstone-grpc/yellowstone-grpc-client-go/proto

# Messages pooled via vtproto's --go-vtproto_opt=pool=<msg>. Scope the
# list to hot-path stream messages and their heavy nested children; the
# pool= option emits a sync.Pool plus ResetVT/ReturnToVTPool helpers,
# and a poolable parent transitively recycles poolable children when
# ReturnToVTPool runs. Small scalar-only wrappers (SubscribeUpdateSlot,
# MessageHeader, CompiledInstruction, Reward, UnixTimestamp, ...) are
# left unpooled — the per-type pool overhead outweighs the alloc saving.
VTPROTO_POOL := \
	github.com/rpcpool/yellowstone-grpc/yellowstone-grpc-client-go/proto.SubscribeUpdate \
	github.com/rpcpool/yellowstone-grpc/yellowstone-grpc-client-go/proto.SubscribeUpdateAccount \
	github.com/rpcpool/yellowstone-grpc/yellowstone-grpc-client-go/proto.SubscribeUpdateAccountInfo \
	github.com/rpcpool/yellowstone-grpc/yellowstone-grpc-client-go/proto.SubscribeUpdateTransaction \
	github.com/rpcpool/yellowstone-grpc/yellowstone-grpc-client-go/proto.SubscribeUpdateTransactionInfo \
	github.com/rpcpool/yellowstone-grpc/yellowstone-grpc-client-go/proto.SubscribeUpdateTransactionStatus \
	github.com/rpcpool/yellowstone-grpc/yellowstone-grpc-client-go/proto.SubscribeUpdateBlock \
	github.com/rpcpool/yellowstone-grpc/yellowstone-grpc-client-go/proto.SubscribeUpdateBlockMeta \
	github.com/rpcpool/yellowstone-grpc/yellowstone-grpc-client-go/proto.SubscribeUpdateEntry \
	github.com/rpcpool/yellowstone-grpc/yellowstone-grpc-client-go/proto.SubscribeUpdateDeshred \
	github.com/rpcpool/yellowstone-grpc/yellowstone-grpc-client-go/proto.SubscribeUpdateDeshredTransaction \
	github.com/rpcpool/yellowstone-grpc/yellowstone-grpc-client-go/proto.SubscribeUpdateDeshredTransactionInfo \
	github.com/rpcpool/yellowstone-grpc/yellowstone-grpc-client-go/proto.ConfirmedTransaction \
	github.com/rpcpool/yellowstone-grpc/yellowstone-grpc-client-go/proto.Transaction \
	github.com/rpcpool/yellowstone-grpc/yellowstone-grpc-client-go/proto.Message \
	github.com/rpcpool/yellowstone-grpc/yellowstone-grpc-client-go/proto.TransactionStatusMeta \
	github.com/rpcpool/yellowstone-grpc/yellowstone-grpc-client-go/proto.InnerInstructions \
	github.com/rpcpool/yellowstone-grpc/yellowstone-grpc-client-go/proto.Rewards

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
		--go-vtproto_out=./proto \
		--go-vtproto_opt=paths=source_relative \
		--go-vtproto_opt=Mgeyser.proto=$(GO_PKG) \
		--go-vtproto_opt=Msolana-storage.proto=$(GO_PKG) \
		--go-vtproto_opt=features=marshal+unmarshal+size+pool+equal+clone \
		$(foreach m,$(VTPROTO_POOL),--go-vtproto_opt=pool=$(m)) \
		--experimental_allow_proto3_optional \
		--proto_path $(PROTO_SRC) $(PROTO_SRC)/*.proto

install-protoc:
	go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.36.9
	go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.5.1
	go install github.com/planetscale/vtprotobuf/cmd/protoc-gen-go-vtproto@latest
	@echo 'ensure $$(go env GOPATH)/bin is on PATH'

build:
	go build ./...

test:
	go test ./...

tidy:
	go mod tidy

.PHONY: protoc install-protoc build test tidy
