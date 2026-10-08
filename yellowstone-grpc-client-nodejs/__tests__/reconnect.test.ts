import { once } from "node:events";
import {
  createServer,
  ServerHttp2Session,
  ServerHttp2Stream,
} from "node:http2";
import Client, {
  AUTORECONNECT_FILTER_KEY,
  ClientReconnectDuplexStream,
  ClientDuplexStream,
  CommitmentLevel,
  ReconnectEvent,
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
function account(lamports: string): SubscribeUpdate {
  return SubscribeUpdate.fromPartial({
    filters: ["accounts"],
    account: {
      slot: "100",
      bankId,
      account: {
        pubkey: Buffer.alloc(32, 1),
        lamports,
        writeVersion: lamports,
      },
    },
  });
}

describe("native reconnect subscriptions", () => {
  test("discards partial banks before replacement updates and forwards request writes", async () => {
    let first!: ServerHttp2Stream;
    const server = await startServer((stream, _request, index) => {
      if (index === 1) {
        first = stream;
        send(stream, account("10"));
      } else {
        send(stream, account("20"));
        send(
          stream,
          SubscribeUpdate.fromPartial({
            filters: [AUTORECONNECT_FILTER_KEY],
            blockMeta: {
              slot: "100",
              bankId,
              blockhash: "winner",
              parentSlot: "99",
            },
          }),
        );
        send(
          stream,
          SubscribeUpdate.fromPartial({
            filters: [AUTORECONNECT_FILTER_KEY],
            slot: { slot: "100", bankId, status: SlotStatus.SLOT_FINALIZED },
          }),
        );
      }
    });
    let stream: ClientReconnectDuplexStream | undefined;
    try {
      const client = new Client(server.endpoint, undefined, undefined, {
        backoff: { initialIntervalMs: 1, maxRetries: 2 },
      });
      await client.connect();
      stream = await client.subscribeWithReconnect(request);
      const events = stream[Symbol.asyncIterator]();
      expect((await events.next()).value).toEqual({
        type: "Update",
        generation: "0",
        update: account("10"),
      });
      fail(first);
      expect((await events.next()).value).toEqual({
        type: "DiscardBanks",
        banks: [{ generation: "0", slot: "100", bankId }],
        reason: "IncompleteDelivery",
        replacement: { generation: "1", fromSlot: "100" },
        winners: [{ type: "Finalized", slot: "100", blockhash: "winner" }],
      });
      expect((await events.next()).value).toEqual({
        type: "Update",
        generation: "1",
        update: account("20"),
      });
      expect(server.requests[1].fromSlot).toBe("100");
      expect(server.requests[1].slots).toHaveProperty(AUTORECONNECT_FILTER_KEY);
      const ping = SubscribeRequest.fromPartial({ ping: { id: 42 } });
      await new Promise<void>((resolve, reject) =>
        stream!.write(ping, (error) => (error ? reject(error) : resolve())),
      );
      const pong = (await events.next()).value as ReconnectEvent;
      expect(pong).toMatchObject({
        type: "Update",
        generation: "1",
        update: { pong: { id: 42 } },
      });
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
        send(stream, account("10"));
      });
      let stream: ClientDuplexStream | ClientReconnectDuplexStream | undefined;
      try {
        const client = new Client(server.endpoint, undefined, undefined, {
          backoff: { maxRetries: reconnect ? 0 : 2 },
        });
        await client.connect();
        stream = reconnect
          ? await client.subscribeWithReconnect(request)
          : await client.subscribe(request);
        const events = stream[Symbol.asyncIterator]();
        const first = (await events.next()).value;
        if (!reconnect) expect(first).toEqual(account("10"));
        fail(nativeStream);
        await expect(events.next()).rejects.toThrow("stream receive failed");
        expect(server.requests).toHaveLength(1);
      } finally {
        stream?.destroy();
        await server.close();
      }
    },
  );

  test("rejects unsupported recovery requests before opening a subscription", async () => {
    const server = await startServer(() => {
      throw new Error("unexpected subscription");
    });
    try {
      const client = new Client(server.endpoint, undefined, undefined);
      await client.connect();
      for (const invalid of [
        { ...request, commitment: CommitmentLevel.CONFIRMED },
        { ...request, fromSlot: "100" },
      ]) {
        await expect(client.subscribeWithReconnect(invalid)).rejects.toThrow(
          "failed to open reconnect subscription",
        );
      }
      expect(server.requests).toHaveLength(0);
    } finally {
      await server.close();
    }
  });

  test("closing a native recovery stream cancels its pending read", async () => {
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
    { enabled: true },
    { enabled: false },
    { slotRetention: 250 },
    { policy: "SkipMissedData" },
  ])(
    "rejects obsolete options %j instead of silently ignoring them",
    async (options) => {
      const client = new Client(
        "http://127.0.0.1:1",
        undefined,
        undefined,
        options as never,
      );
      await expect(client.connect()).rejects.toThrow(
        "use subscribeWithReconnect()",
      );
    },
  );
});

describe("reconnect stream lifecycle", () => {
  test("stops native reads at the readable high-water mark", async () => {
    const native = {
      read: jest.fn(async () => ({
        type: "Update",
        generation: "0",
        update: SubscribeUpdate.encode(account("10")).finish(),
      })),
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
        generation: "0",
        update: account("10"),
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
    resolveRead({
      type: "Update",
      generation: "0",
      update: SubscribeUpdate.encode(account("10")).finish(),
    });
    await new Promise<void>((resolve) => setImmediate(resolve));
    expect(stream.readableLength).toBe(0);
    expect(native.close).toHaveBeenCalledTimes(1);
  });
});
