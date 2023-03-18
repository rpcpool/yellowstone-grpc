# TypeScript gRPC client example

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

Run it:

```
node . --endpoint https://api.rpcpool.com --x-token <token> --slots
```
