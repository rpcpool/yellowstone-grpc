import Client from "../src"

describe("connection management and unary gRPC method tests", () => {
  const TEST_TIMEOUT = 100000;

  // .env
  const {
    TEST_ENDPOINT: endpoint,
    TEST_TOKEN: xToken
  } = process.env;

  // Use options sensible defaults.
  const channelOptions = {};

  test("connect() initializes the gRPC client", async () => {
    const client = new Client(endpoint, xToken, channelOptions);
    expect(client.isConnected()).toBe(false);
    await client.connect();
    expect(client.isConnected()).toBe(true);
  }, TEST_TIMEOUT);

  test("subscribe() auto-connects when connect() has not been called explicitly", async () => {
    const client = new Client(endpoint, xToken, channelOptions);
    expect(client.isConnected()).toBe(false);

    // subscribe() should auto-connect
    const stream = await client.subscribe();
    expect(client.isConnected()).toBe(true);

    const request = {
      slots: {
        client: {}
      },
      commitment: 2
    };

    let response: any;

    stream.write(request, (err) => {
      if (err) {
        console.error(`error writing to stream: ${err}`)
      }
    });

    stream.once("data", (data) => {
      response = data;
      stream.destroy();
    });

    const streamClosed = new Promise<void>((resolve, reject) => {
      stream.on("error", (error) => {
        reject(error);
      });
      stream.on("end", () => {
        resolve();
      });
      stream.on("close", () => {
        resolve();
      });
    });

    await streamClosed;

    expect(response).toBeDefined();
    expect(typeof response.slot.slot).toBe("bigint");
  }, TEST_TIMEOUT);

  test("explicit connect() followed by subscribe() works correctly", async () => {
    const client = new Client(endpoint, xToken, channelOptions);
    await client.connect();

    const stream = await client.subscribe();
    const request = {
      slots: {
        client: {}
      },
      commitment: 2
    };

    let response: any;

    stream.write(request, (err) => {
      if (err) {
        console.error(`error writing to stream: ${err}`)
      }
    });

    stream.once("data", (data) => {
      response = data;
      stream.destroy();
    });

    const streamClosed = new Promise<void>((resolve, reject) => {
      stream.on("error", (error) => {
        reject(error);
      });
      stream.on("end", () => {
        resolve();
      });
      stream.on("close", () => {
        resolve();
      });
    });

    await streamClosed;

    expect(response).toBeDefined();
    expect(typeof response.slot.slot).toBe("bigint");
  }, TEST_TIMEOUT);

  test("getSlot() throws 'Client not connected' when connect() was not called", async () => {
    const client = new Client(endpoint, xToken, channelOptions);
    await expect(client.getSlot()).rejects.toThrow("Client not connected. Call connect() first");
  }, TEST_TIMEOUT);

  test("getVersion() throws 'Client not connected' when connect() was not called", async () => {
    const client = new Client(endpoint, xToken, channelOptions);
    await expect(client.getVersion()).rejects.toThrow("Client not connected. Call connect() first");
  }, TEST_TIMEOUT);

  test("getBlockHeight() throws 'Client not connected' when connect() was not called", async () => {
    const client = new Client(endpoint, xToken, channelOptions);
    await expect(client.getBlockHeight()).rejects.toThrow("Client not connected. Call connect() first");
  }, TEST_TIMEOUT);

  test("getLatestBlockhash() throws 'Client not connected' when connect() was not called", async () => {
    const client = new Client(endpoint, xToken, channelOptions);
    await expect(client.getLatestBlockhash()).rejects.toThrow("Client not connected. Call connect() first");
  }, TEST_TIMEOUT);

  test("ping() throws 'Client not connected' when connect() was not called", async () => {
    const client = new Client(endpoint, xToken, channelOptions);
    await expect(client.ping(1)).rejects.toThrow("Client not connected. Call connect() first");
  }, TEST_TIMEOUT);

  test("getSlot() returns current slot as a string after connect()", async () => {
    const client = new Client(endpoint, xToken, channelOptions);
    await client.connect();
    const slot = await client.getSlot();
    expect(typeof slot).toBe("string");
    expect(BigInt(slot)).toBeGreaterThan(0n);
  }, TEST_TIMEOUT);

  test("getVersion() returns a non-empty version string after connect()", async () => {
    const client = new Client(endpoint, xToken, channelOptions);
    await client.connect();
    const version = await client.getVersion();
    expect(typeof version).toBe("string");
    expect(version.length).toBeGreaterThan(0);
  }, TEST_TIMEOUT);

  test("getBlockHeight() returns block height as a string after connect()", async () => {
    const client = new Client(endpoint, xToken, channelOptions);
    await client.connect();
    const blockHeight = await client.getBlockHeight();
    expect(typeof blockHeight).toBe("string");
    expect(BigInt(blockHeight)).toBeGreaterThan(0n);
  }, TEST_TIMEOUT);

  test("getLatestBlockhash() returns valid blockhash response after connect()", async () => {
    const client = new Client(endpoint, xToken, channelOptions);
    await client.connect();
    const response = await client.getLatestBlockhash();
    expect(typeof response.blockhash).toBe("string");
    expect(response.blockhash.length).toBeGreaterThan(0);
    expect(typeof response.slot).toBe("string");
    expect(BigInt(response.slot)).toBeGreaterThan(0n);
    expect(typeof response.lastValidBlockHeight).toBe("string");
    expect(BigInt(response.lastValidBlockHeight)).toBeGreaterThan(0n);
  }, TEST_TIMEOUT);

  test("ping() returns the same count that was sent after connect()", async () => {
    const client = new Client(endpoint, xToken, channelOptions);
    await client.connect();
    const count = await client.ping(42);
    expect(count).toBe(42);
  }, TEST_TIMEOUT);
});
