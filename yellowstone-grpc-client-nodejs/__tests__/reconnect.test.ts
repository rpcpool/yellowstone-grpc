import { once } from "node:events";
import {
  createServer,
  ServerHttp2Session,
  ServerHttp2Stream,
} from "node:http2";
import Client, {
  ClientDuplexStream,
  ClientReconnectDuplexStream,
  RECONNECT_FILTER_KEY,
  ReconnectOptions,
  SlotStatus,
  SubscribeRequest,
  SubscribeUpdate,
} from "../src";
import { GrpcClient } from "../src/napi";

function send(stream: ServerHttp2Stream, update: SubscribeUpdate) {
  const payload = SubscribeUpdate.encode(update).finish();
  const header = Buffer.alloc(5);
  header.writeUInt32BE(payload.length, 1);
  stream.write(Buffer.concat([header, payload]));
}

function fail(stream: ServerHttp2Stream) {
  stream.once("wantTrailers", () => {
    stream.sendTrailers({
      "grpc-status": "14",
      "grpc-message": "test disconnect",
    });
  });
  stream.end();
}

async function startServer(
  onSubscribe: (
    stream: ServerHttp2Stream,
    request: SubscribeRequest,
    index: number,
  ) => void,
) {
  const server = createServer();
  const sessions = new Set<ServerHttp2Session>();
  const requests: SubscribeRequest[] = [];
  // ts-proto drops map entries whose value is an empty message, such as a block meta filter;
  // the raw bytes keep them.
  const rawRequests: Buffer[] = [];
  server.on("session", (session) => {
    sessions.add(session);
    session.on("close", () => sessions.delete(session));
    session.on("error", () => {});
  });
  server.on("stream", (stream: ServerHttp2Stream, headers) => {
    stream.on("error", () => {});
    if (headers[":path"] !== "/geyser.Geyser/Subscribe") {
      stream.respond({
        ":status": 200,
        "content-type": "application/grpc",
        "grpc-status": "12",
      });
      stream.end();
      return;
    }
    let pending = Buffer.alloc(0);
    let opened = false;
    stream.on("data", (chunk: Buffer) => {
      pending = Buffer.concat([pending, chunk]);
      while (pending.length >= 5) {
        const size = pending.readUInt32BE(1);
        if (pending.length < size + 5) return;
        rawRequests.push(Buffer.from(pending.subarray(5, size + 5)));
        const request = SubscribeRequest.decode(pending.subarray(5, size + 5));
        pending = pending.subarray(size + 5);
        requests.push(request);
        if (!opened) {
          opened = true;
          stream.respond(
            { ":status": 200, "content-type": "application/grpc" },
            { waitForTrailers: true },
          );
          onSubscribe(stream, request, requests.length);
        } else if (request.ping) {
          send(
            stream,
            SubscribeUpdate.fromPartial({ pong: { id: request.ping.id } }),
          );
        }
      }
    });
  });
  server.listen(0, "127.0.0.1");
  await once(server, "listening");
  const address = server.address() as { port: number };
  return {
    endpoint: `http://127.0.0.1:${address.port}`,
    requests,
    rawRequests,
    async close() {
      for (const session of sessions) session.destroy();
      await new Promise<void>((resolve) => server.close(() => resolve()));
    },
  };
}

const bankId = "18446744073709551614";
const request = SubscribeRequest.fromPartial({
  accounts: { accounts: { account: ["11111111111111111111111111111111"] } },
});

function account(slot: string, lamports: string): SubscribeUpdate {
  return SubscribeUpdate.fromPartial({
    filters: ["accounts"],
    account: {
      slot,
      bankId,
      account: {
        pubkey: Buffer.alloc(32, 1),
        lamports,
        writeVersion: lamports,
      },
    },
  });
}

/** How many filters named `RECONNECT_FILTER_KEY` an encoded request holds. */
function reconnectFilters(raw: Buffer): number {
  return raw.toString("latin1").split(RECONNECT_FILTER_KEY).length - 1;
}

/** A Confirmed status only the hidden reconnect filter matched. */
function hiddenConfirmed(slot: string): SubscribeUpdate {
  return SubscribeUpdate.fromPartial({
    filters: [RECONNECT_FILTER_KEY],
    slot: { slot, bankId: slot, status: SlotStatus.SLOT_CONFIRMED },
  });
}

function write(stream: ClientReconnectDuplexStream, request: SubscribeRequest) {
  return new Promise<void>((resolve, reject) =>
    stream.write(request, (error) => (error ? reject(error) : resolve())),
  );
}

async function connect(endpoint: string, options?: ReconnectOptions) {
  const client = new Client(endpoint, undefined, undefined, {
    backoff: { initialIntervalMs: 1, maxRetries: 2 },
    ...options,
  });
  await client.connect();
  return client;
}

describe("native reconnect subscriptions", () => {
  test("reports the partial bank, then replays from the last confirmed slot", async () => {
    let first!: ServerHttp2Stream;
    const server = await startServer((stream, _request, index) => {
      if (index === 1) {
        first = stream;
        send(stream, hiddenConfirmed("99"));
        send(stream, account("100", "10"));
      } else {
        send(stream, account("100", "20"));
      }
    });
    let stream: ClientReconnectDuplexStream | undefined;
    try {
      const client = await connect(server.endpoint);
      stream = await client.subscribeWithReconnect(request);
      const events = stream[Symbol.asyncIterator]();
      // The hidden status never reaches the user.
      expect((await events.next()).value).toEqual({
        type: "Update",
        update: account("100", "10"),
      });
      fail(first);
      expect((await events.next()).value).toEqual({
        type: "Disconnected",
        partialBanks: [{ slot: "100", bankId }],
        replayFromSlot: "99",
      });
      expect((await events.next()).value).toEqual({
        type: "Update",
        update: account("100", "20"),
      });

      expect(server.requests[0].fromSlot).toBeUndefined();
      expect(server.requests[1].fromSlot).toBe("99");
      for (const [index, sent] of server.requests.entries()) {
        expect(sent.accounts).toHaveProperty("accounts");
        expect(sent.slots).toHaveProperty(RECONNECT_FILTER_KEY);
        // One for the slot filter, one for the block meta filter.
        expect(reconnectFilters(server.rawRequests[index])).toBe(2);
      }

      await write(stream, SubscribeRequest.fromPartial({ ping: { id: 42 } }));
      expect((await events.next()).value).toMatchObject({
        type: "Update",
        update: { pong: { id: 42 } },
      });
    } finally {
      stream?.destroy();
      await server.close();
    }
  });

  test("without replay, resumes at the tip and drops the rest of the partial bank", async () => {
    let first!: ServerHttp2Stream;
    const server = await startServer((stream, _request, index) => {
      if (index === 1) {
        first = stream;
        send(stream, hiddenConfirmed("99"));
        send(stream, account("100", "10"));
      } else {
        // The rest of the reported bank, then another bank.
        send(stream, account("100", "20"));
        send(stream, account("101", "30"));
      }
    });
    let stream: ClientReconnectDuplexStream | undefined;
    try {
      const client = await connect(server.endpoint, { policy: "NoReplay" });
      stream = await client.subscribeWithReconnect(request);
      const events = stream[Symbol.asyncIterator]();
      expect((await events.next()).value).toEqual({
        type: "Update",
        update: account("100", "10"),
      });
      fail(first);
      expect((await events.next()).value).toEqual({
        type: "Disconnected",
        partialBanks: [{ slot: "100", bankId }],
        replayFromSlot: null,
      });
      expect((await events.next()).value).toEqual({
        type: "Update",
        update: account("101", "30"),
      });
      expect(server.requests[1].fromSlot).toBeUndefined();
    } finally {
      stream?.destroy();
      await server.close();
    }
  });

  test("a filter written to the stream keeps the reconnect filters and survives a reconnect", async () => {
    let first!: ServerHttp2Stream;
    const server = await startServer((stream, _request, index) => {
      if (index === 1) {
        first = stream;
        send(stream, account("100", "10"));
      } else {
        send(stream, account("101", "20"));
      }
    });
    let stream: ClientReconnectDuplexStream | undefined;
    try {
      const client = await connect(server.endpoint);
      stream = await client.subscribeWithReconnect(request);
      const events = stream[Symbol.asyncIterator]();
      await events.next();

      const updated = SubscribeRequest.fromPartial({
        accounts: {
          accounts: { account: ["11111111111111111111111111111111"] },
        },
        slots: { late: { filterByCommitment: true } },
      });
      await write(stream, updated);
      // The fake server records the write on the live connection.
      while (server.requests.length < 2) {
        await new Promise<void>((resolve) => setImmediate(resolve));
      }
      expect(server.requests[1].slots).toHaveProperty("late");
      expect(reconnectFilters(server.rawRequests[1])).toBe(2);

      fail(first);
      expect((await events.next()).value).toMatchObject({
        type: "Disconnected",
      });
      expect((await events.next()).value).toEqual({
        type: "Update",
        update: account("101", "20"),
      });
      // The reconnect subscribes with the latest request.
      expect(server.requests[2].slots).toHaveProperty("late");
      expect(reconnectFilters(server.rawRequests[2])).toBe(2);
    } finally {
      stream?.destroy();
      await server.close();
    }
  });

  test.each([false, true])(
    "surfaces terminal errors (reconnect=%s)",
    async (reconnect) => {
      let nativeStream!: ServerHttp2Stream;
      const server = await startServer((stream) => {
        nativeStream = stream;
        send(stream, account("100", "10"));
      });
      let stream: ClientDuplexStream | ClientReconnectDuplexStream | undefined;
      try {
        const client = await connect(server.endpoint, {
          backoff: { maxRetries: reconnect ? 0 : 2 },
        });
        stream = reconnect
          ? await client.subscribeWithReconnect(request)
          : await client.subscribe(request);
        const events = stream[Symbol.asyncIterator]();
        await events.next();
        fail(nativeStream);
        await expect(events.next()).rejects.toThrow("stream receive failed");
        expect(server.requests).toHaveLength(1);
      } finally {
        stream?.destroy();
        await server.close();
      }
    },
  );

  test("rejects the reserved filter name before opening a subscription", async () => {
    const server = await startServer(() => {
      throw new Error("unexpected subscription");
    });
    try {
      const client = await connect(server.endpoint);
      await expect(
        client.subscribeWithReconnect({
          ...request,
          slots: { [RECONNECT_FILTER_KEY]: {} },
        }),
      ).rejects.toThrow("reserved");
      expect(server.requests).toHaveLength(0);
    } finally {
      await server.close();
    }
  });

  test("closing a native reconnect stream cancels its pending read", async () => {
    const server = await startServer(() => {});
    let stream:
      Awaited<ReturnType<GrpcClient["subscribeWithReconnect"]>> | undefined;
    try {
      const client = await GrpcClient.new(server.endpoint);
      stream = await client.subscribeWithReconnect(
        Buffer.from(SubscribeRequest.encode(request).finish()),
      );
      const pending = stream.read();
      stream.close();
      expect(await pending).toBeNull();
      expect(() =>
        stream!.writeRaw(
          Buffer.from(SubscribeRequest.encode(request).finish()),
        ),
      ).toThrow("closed subscription");
    } finally {
      stream?.close();
      await server.close();
    }
  });

  test.each([
    [{ enabled: true }, "use subscribeWithReconnect()"],
    [{ slotRetention: 250 }, "use subscribeWithReconnect()"],
    [{ policy: "NoReplay", slotsBehind: 1 }, "slotsBehind"],
    [{ policy: "SkipMissedData" }, ""],
  ])("rejects invalid options %j", async (options, message) => {
    const client = new Client(
      "http://127.0.0.1:1",
      undefined,
      undefined,
      options as never,
    );
    await expect(client.connect()).rejects.toThrow(message);
  });
});

describe("reconnect stream lifecycle", () => {
  const nativeUpdate = () => ({
    type: "Update",
    update: SubscribeUpdate.encode(account("100", "10")).finish(),
  });

  test("decodes a Disconnected event", async () => {
    const native = {
      read: jest
        .fn()
        .mockResolvedValueOnce({
          type: "Disconnected",
          partialBanks: [{ slot: "100", bankId }],
        })
        // Stays pending: ending the stream would close the iterator early.
        .mockReturnValue(new Promise(() => {})),
      close: jest.fn(),
    };
    const stream = new ClientReconnectDuplexStream(native, {
      objectMode: true,
    });
    const events = stream[Symbol.asyncIterator]();
    expect((await events.next()).value).toEqual({
      type: "Disconnected",
      partialBanks: [{ slot: "100", bankId }],
      replayFromSlot: null,
    });
    stream.destroy();
  });

  test("stops native reads at the readable high-water mark", async () => {
    const native = {
      read: jest.fn(async () => nativeUpdate()),
      close: jest.fn(),
    };
    const stream = new ClientReconnectDuplexStream(native, {
      objectMode: true,
      highWaterMark: 1,
    });
    try {
      stream.read(0);
      await new Promise<void>((resolve) => setImmediate(resolve));
      expect(native.read).toHaveBeenCalledTimes(1);
      expect(stream.readableLength).toBe(1);
      expect(stream.read()).toEqual({
        type: "Update",
        update: account("100", "10"),
      });
      await new Promise<void>((resolve) => setImmediate(resolve));
      expect(native.read).toHaveBeenCalledTimes(2);
    } finally {
      stream.destroy();
    }
    expect(native.close).toHaveBeenCalledTimes(1);
  });

  test("allows only one pending read and ignores completion after destroy", async () => {
    let resolveRead!: (value: unknown) => void;
    const native = {
      read: jest.fn(
        () =>
          new Promise((resolve) => {
            resolveRead = resolve;
          }),
      ),
      close: jest.fn(),
    };
    const stream = new ClientReconnectDuplexStream(native, {
      objectMode: true,
    });
    stream._read(0);
    stream._read(0);
    expect(native.read).toHaveBeenCalledTimes(1);
    stream.destroy();
    resolveRead(nativeUpdate());
    await new Promise<void>((resolve) => setImmediate(resolve));
    expect(stream.readableLength).toBe(0);
    expect(native.close).toHaveBeenCalledTimes(1);
  });
});
