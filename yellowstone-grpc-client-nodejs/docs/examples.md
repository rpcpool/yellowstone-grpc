# How to run examples

1. Install `node` `v22.14.0` or later. Make sure it's the node version you're using.

2. In the root directory of the repository (where `Makefile` is found)

```bash
make solana-encoding-wasm-install-dependencies && make solana-encoding-wasm-build
```

3. Change directory into the `yellowstone-grpc-client-nodejs` package and run `npm install && npm run build`;

```bash
cd yellowstone-grpc-client-nodejs && npm install && npm build
```

4. Change directory into `examples/typescript` and test the following command to run the example.

```bash
tsx src/client.ts --endpoint "YOUR ENDPOINT HERE WITH PORT" \
  --x-token "USE YOUR TOKEN HERE IF NEEDED" \
  --commitment processed subscribe \
  --blocks --slots --transactions TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA
```
