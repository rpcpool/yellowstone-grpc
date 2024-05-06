# Python example

## Instruction

This Python example uses [grpc.io](https://grpc.io/) library.
It assumes your CA trust store on your machine allows trust the CA from your RPC endpoint.

## Installation

Create a virtual environment and install its dependencies:
```bash
$ python -m venv venv
$ . venv/bin/activate
(venv) $ python -m pip install -U pip
(venv) $ python -m pip install -r requirements.txt
```

## Launch the helloworld_geyser

Print the usage:

```bash
(venv) $ python helloworld_geyser.py --help
Usage: helloworld_geyser.py [OPTIONS]

  Simple program to get the latest solana slot number

Options:
  --rpc-fqdn TEXT  Fully Qualified domain name of your RPC endpoint
  --x-token TEXT   x-token to authenticate each gRPC call
  --help           Show this message and exit.
```

- `rpc-fqdn`: is the fully qualified domain name without the `https://`, such as `index.rpcpool.com`.
- `x-token` : is the x-token to authenticate yourself to the RPC node.

Here is a full example:

```bash
(venv) $ python helloworld_geyser.py --rpc-fqdn 'index.rpcpool.com' --x-token '2625ae71-0823-41b3-b3bc-4ff89d762d52'
slot: 264236514

```

**NOTE**: `2625ae71-0823-41b3-b3bc-4ff89d762d52` is a fake x-token, you need to provide your own token.

## Generate gRPC service and request signatures

The library `grpcio` generates the stubs for you.

From the directory of `helloword_geyser.py` you can generate all the stubs and data types using the following command:

```bash
(venv) $ python -m grpc_tools.protoc -I../../yellowstone-grpc-proto/proto/ --python_out=. --pyi_out=. --grpc_python_out=. ../../yellowstone-grpc-proto/proto/*
```

This will generate:
- geyser_pb2.py
- geyser_pb2.pyi
- geyser_pb2_grpc.py
- solana_storage_pb2.py
- solana_storage_pb2.pyi
- solana_storage_pb2_grpc.py

Which you can then import into your code.


## Useful documentation for grpcio authentication process

- [secure_channel](https://grpc.github.io/grpc/python/grpc.html#create-client-credentials)

- [extend auth method via call credentials](https://grpc.io/docs/guides/auth/#extending-grpc-to-support-other-authentication-mechanisms)