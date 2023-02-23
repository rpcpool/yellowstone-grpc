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

You can run `make` to update the protofiles.
