# Golang client for Solana gRPC interface

This is a sample golang client for the Solana gRPC interface.

Requires golang 1.17

Sample usage:

```
go1.17 run ./cmd/grpc-client/ -endpoint https://api.rpcpool.com:443 -x-token <token> -slots
```

You can also make non SSL connections:

```
go1.17 run ./cmd/grpc-client/ -endpoint http://api.rpcpool.com:80 -x-token <token> -blocks
````

## Updating protofiles

Make sure you have protoc installed for go:

```
$ go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.28
$ go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.2
```

If you have `dnf` package manager:

```
dnf install golang-google-grpc golang-google-protobuf
```

You can run `make` to update the protofiles.
