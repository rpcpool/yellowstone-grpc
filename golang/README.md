# Golang client for Solana gRPC interface

This is a sample golang client for the Solana gRPC interface.

Requires golang 1.17

Sample usage:

```
go1.17 run ./cmd/grpc-client/ -grpc api.rpcpool.com:443 -token <token> -slots
```

You can also make non SSL connections:

```
go1.17 run ./cmd/grpc-client/ -insecure -grpc api.rpcpool.com:80 -token <token> -blocks
````

## Updating protofiles

You can run `make` to update the protofiles.
