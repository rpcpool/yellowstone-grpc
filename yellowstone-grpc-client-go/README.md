# Yellowstone Go gRPC client

Go client for the Yellowstone Geyser gRPC interface. Counterpart to
[yellowstone-grpc-client-nodejs](../yellowstone-grpc-client-nodejs) and
[yellowstone-grpc-client](../yellowstone-grpc-client) (Rust).

It is a thin wrapper around the generated gRPC client with:

- fluent `Builder` for TLS / keepalive / x-token / compression / message
  size / HTTP/2 window / buffer size / TCP keepalive / TCP_NODELAY
- `Connect` (TCP, inferred from URL scheme) and `ConnectUDS(path)` for
  Unix Domain Sockets
- `x-token` and `x-request-snapshot` injected on every call via gRPC
  interceptors (works on both plaintext and TLS connections)
- helpers for the two bidi streams (`Subscribe`, `SubscribeDeshred`),
  including `SubscribeOnce` / `SubscribeDeshredOnce` for the common case
  of a single initial request
- typed wrappers around every unary RPC (`Ping`, `GetLatestBlockhash`,
  `GetBlockHeight`, `GetSlot`, `IsBlockhashValid`, `GetVersion`,
  `SubscribeReplayInfo`)
- `HealthCheck` / `HealthWatch` against the `geyser.Geyser` service,
  mirroring the Rust client's health RPCs
- `WaitReady(ctx)` on the client for eager-connect semantics (since
  `grpc.NewClient` is lazy by default)

## Install

```sh
go get github.com/rpcpool/yellowstone-grpc/yellowstone-grpc-client-go
```

## Usage

```go
package main

import (
    "context"
    "log"

    client "github.com/rpcpool/yellowstone-grpc/yellowstone-grpc-client-go"
    pb "github.com/rpcpool/yellowstone-grpc/yellowstone-grpc-client-go/proto"
)

func main() {
    ctx := context.Background()

    c, err := client.NewBuilder("https://api.rpcpool.com").
        WithXToken("my-token").
        WithKeepaliveParams(client.DefaultKeepalive).
        Connect(ctx)
    if err != nil {
        log.Fatal(err)
    }
    defer c.Close()

    slot, err := c.GetSlot(ctx, client.Commitment(pb.CommitmentLevel_CONFIRMED))
    if err != nil {
        log.Fatal(err)
    }
    log.Printf("slot=%d", slot.Slot)

    stream, err := c.SubscribeOnce(ctx, &pb.SubscribeRequest{
        Slots: map[string]*pb.SubscribeRequestFilterSlots{
            "slots": {},
        },
    })
    if err != nil {
        log.Fatal(err)
    }
    for {
        update, err := stream.Recv()
        if err != nil {
            log.Fatal(err)
        }
        log.Printf("%v", update)
    }
}
```

A runnable CLI example lives at [examples/golang](../examples/golang).

### Unix Domain Socket

```go
c, err := client.NewBuilder("ignored").ConnectUDS(ctx, "/tmp/yellowstone.sock")
```

`ConnectUDS` forces plaintext and ignores TCP-only options.

### Eager connect and health

```go
c, _ := client.NewBuilder("https://...").Connect(ctx)
if err := c.WaitReady(ctx); err != nil {
    log.Fatal(err)
}
resp, _ := c.HealthCheck(ctx)
log.Printf("health=%s", resp.Status)
```

### HTTP/2 and TCP tuning

```go
client.NewBuilder("https://...").
    WithInitialConnWindowSize(1 << 24).
    WithInitialWindowSize(1 << 22).
    WithReadBufferSize(256 << 10).
    WithTCPKeepalive(30 * time.Second).
    WithTCPNoDelay(true).
    Connect(ctx)
```

## Regenerating protobuf

The `proto/` package is generated from
[../yellowstone-grpc-proto/proto](../yellowstone-grpc-proto/proto).

```sh
make install-protoc  # once
make protoc
```

The Makefile passes `--go_opt=M<file>=<this module's proto path>` so the
generated code lives inside this module (rather than the examples
module), which is what allows this library to import it cleanly.
