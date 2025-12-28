# Yellowstone Node.js gRPC client

This library implements a client for streaming account updates for backend applications.

You can find more information and documentation on the [Triton One website](https://docs.triton.one/project-yellowstone/introduction).

## Prerequisites

You need to have the latest version of `protoc` installed.
Please refer to the [installation guide](https://grpc.io/docs/protoc-installation/) on the Protobuf website.

## Usage

Install required dependencies by running

```bash
npm install
```

Build the project (this will generate the gRPC client and compile TypeScript):

```
npm run build
```

Please refer to [examples/typescript](../examples/typescript/README.md) for some usage examples.

## Troubleshooting

### For macOS:

You might have to run `npm run build` with `RUSTFLAGS="-Clink-arg=-undefined -Clink-arg=dynamic_lookup"` to skip the strict linkers from failing the build step and resolve `dylib`s via runtime.

```bash
RUSTFLAGS="-Clink-arg=-undefined -Clink-arg=dynamic_lookup" npm run build
```

## Working

Since the start, the `@triton-one/yellowstone-grpc` package has used the `@grpc/grpc-js` lib for gRPC types enforcement, connection and subscription management. This hit a bottleneck, described in [this blog](https://blog.triton.one/supercharging-the-javascript-sdk-with-napi/)

From `v5.0.0` the [napi-rs](https://github.com/napi-rs/napi-rs) framework is used for gRPC connection and subscription management. It's described into [this blog](https://blog.triton.one/supercharging-the-javascript-sdk-with-napi/)

These changes are internal to the SDK and do not have any breaking changes for client code. If you face any issues, please open an issue

The [napi-rs](https://github.com/napi-rs/napi-rs) based implementation is inspired from the implemenation of the [LaserStream SDK](https://github.com/helius-labs/laserstream-sdk)
