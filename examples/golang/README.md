# Golang client example for Yellowstone gRPC

## DISCLAIMER

This example may contain errors or lag behind the latest stable version —
use it only as a reference for what a subscription looks like. For a
well-tested production example, see the Rust implementation.

<hr>

Sample Go client for the Yellowstone gRPC interface. Built on top of the
[yellowstone-grpc-client-go](../../yellowstone-grpc-client-go) library,
which provides the typed client, builder, auth interceptor, and generated
protobuf bindings.

Requires Go 1.24.

## Running

```
go run ./cmd/grpc-client -endpoint https://api.rpcpool.com:443 -x-token <token> -slots
```

Plaintext:

```
go run ./cmd/grpc-client -endpoint http://api.rpcpool.com:80 -x-token <token> -blocks
```

## Updating the generated protobuf

The proto files now live in the library. To regenerate them:

```
cd ../../yellowstone-grpc-client-go
make install-protoc  # once
make protoc
```
