# Golang client for Solana gRPC interface

## DISCLAIMER

This example can contains errors or be behind of the latest stable version, please use it only as an example of how your subscription can looks like. If you want well tested production ready example, please check our implementation on Rust.

<hr>

This is a sample golang client for the Solana gRPC interface.

Requires golang 1.21

Sample usage:

```
go run ./cmd/grpc-client/ -endpoint https://api.rpcpool.com:443 -x-token <token> -slots
```

You can also make non SSL connections:

```
go run ./cmd/grpc-client/ -endpoint http://api.rpcpool.com:80 -x-token <token> -blocks
```

## Updating protofiles

Make sure you have protoc installed:

```bash
$ protoc --version
libprotoc 28.3
```

And the golang plugins:

```bash
$ go list -f '{{.Path}} {{.Version}}' -m google.golang.org/protobuf
google.golang.org/protobuf v1.35.1
$ go list -f '{{.Path}} {{.Version}}' -m google.golang.org/grpc
google.golang.org/grpc v1.67.1
```

If you don't have the protobuf go plugins installed, you can install them with:

```
$ go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.35.1
$ go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.5.1
```

If you have `dnf` package manager:

```
dnf install golang-google-grpc golang-google-protobuf
```

You can run `make` to update the protofiles.
