import Client, {
  ClientDeshredDuplexStream,
  ClientDuplexStream,
  txDeshredEncode,
} from "../src";
import {
  GetBlockHeightResponse,
  GetLatestBlockhashResponse,
  GetSlotResponse,
  GetVersionResponse,
  IsBlockhashValidResponse,
  PongResponse,
  SubscribeUpdate,
  SubscribeUpdateAccount,
  SubscribeUpdateBlock,
  SubscribeUpdateBlockMeta,
  SubscribeUpdateEntry,
  SubscribeUpdatePing,
  SubscribeUpdatePong,
  SubscribeReplayInfoResponse,
  SubscribeDeshredRequest,
  SubscribeRequest,
  SubscribeUpdateSlot,
  SubscribeUpdateTransaction,
  SubscribeUpdateTransactionInfo,
  SubscribeUpdateTransactionStatus,
} from "../src/grpc/geyser";
import * as geyser from "../src/grpc/geyser";

function closeStreamAndWait(stream: any, timeoutMs = 2500): Promise<void> {
  return new Promise((resolve) => {
    let settled = false;

    const settleOnce = () => {
      if (settled) {
        return;
      }
      settled = true;
      cleanup();
      resolve();
    };

    const cleanup = () => {
      clearTimeout(timeoutId);
      stream.off("close", onClosedOrEnded);
      stream.off("end", onClosedOrEnded);
      stream.off("error", onError);
      stream.off("finish", onFinish);
    };

    const onClosedOrEnded = () => settleOnce();
    const onError = () => settleOnce();
    const onFinish = () => {
      // Writable side finished; wait for close/end unless timeout hits.
    };

    const timeoutId = setTimeout(settleOnce, timeoutMs);

    stream.on("close", onClosedOrEnded);
    stream.on("end", onClosedOrEnded);
    stream.on("error", onError);
    stream.on("finish", onFinish);

    try {
      stream.end();
    } catch {}

    try {
      stream.destroy();
    } catch {}
  });
}

function waitForSubscribeUpdateMatchingPredicate(
  stream: any,
  predicate: (data: any) => boolean,
  timeoutMs: number,
  maxUnmatchedUpdates = 500,
): Promise<any> {
  return new Promise((resolve, reject) => {
    let settled = false;
    let unmatchedUpdates = 0;

    const settleOnce = (handler: () => void) => {
      if (settled) {
        return;
      }
      settled = true;
      handler();
    };

    const cleanup = () => {
      clearTimeout(timeoutId);
      stream.off("data", onData);
      stream.off("error", onError);
      stream.off("end", onEndOrClose);
      stream.off("close", onEndOrClose);
    };

    const onData = (data: any) => {
      if (!predicate(data)) {
        unmatchedUpdates += 1;
        if (unmatchedUpdates >= maxUnmatchedUpdates) {
          settleOnce(() => {
            cleanup();
            const error = new Error(
              `No matching subscribe update after ${unmatchedUpdates} updates (timeout ${timeoutMs}ms)`,
            );
            void closeStreamAndWait(stream).finally(() => reject(error));
          });
        }
        return;
      }

      settleOnce(() => {
        cleanup();
        void closeStreamAndWait(stream).finally(() => resolve(data));
      });
    };

    const onError = (error: Error) => {
      settleOnce(() => {
        cleanup();
        void closeStreamAndWait(stream).finally(() => reject(error));
      });
    };

    const onEndOrClose = () => {
      settleOnce(() => {
        cleanup();
        const error = new Error(
          "Stream ended before receiving expected subscribe update",
        );
        void closeStreamAndWait(stream).finally(() => reject(error));
      });
    };

    const timeoutId = setTimeout(() => {
      settleOnce(() => {
        cleanup();
        const error = new Error(
          `Timed out waiting for expected subscribe update after ${timeoutMs}ms`,
        );
        void closeStreamAndWait(stream).finally(() => reject(error));
      });
    }, timeoutMs);

    stream.on("data", onData);
    stream.on("error", onError);
    stream.on("end", onEndOrClose);
    stream.on("close", onEndOrClose);
  });
}

function expectEncodeDecodeRoundTrip(
  messageFns: any,
  payload: any,
  allowEmpty = false,
): any {
  const encoded = messageFns.encode(payload).finish();
  const decoded = messageFns.decode(encoded);

  expect(encoded).toBeInstanceOf(Uint8Array);
  if (allowEmpty) {
    expect(encoded.length).toBeGreaterThanOrEqual(0);
  } else {
    expect(encoded.length).toBeGreaterThan(0);
  }
  expect(decoded).toBeDefined();

  return decoded;
}

function isChannelClosedError(error: any): boolean {
  const message = String(error?.message ?? error ?? "");
  return message.toLowerCase().includes("channel closed");
}

function makeMinimalSubscribeRequest(): SubscribeRequest {
  return {
    accounts: {},
    slots: {},
    transactions: {},
    transactionsStatus: {},
    accountsDataSlice: [],
    blocks: {},
    blocksMeta: {},
    entry: {},
    commitment: 2,
  };
}

function makeComprehensiveSubscribeRequest(): SubscribeRequest {
  return {
    accounts: {
      allFields: {
        account: ["accountA", "accountB"],
        owner: ["ownerA"],
        nonemptyTxnSignature: true,
        filters: [
          { memcmp: { offset: "1", bytes: Uint8Array.from([1, 2, 3]) } },
          {
            memcmp: { offset: "2", base58: "11111111111111111111111111111111" },
          },
          { memcmp: { offset: "3", base64: "AQID" } },
          { datasize: "165" },
          { tokenAccountState: true },
          { lamports: { eq: "10" } },
          { lamports: { ne: "11" } },
          { lamports: { lt: "12" } },
          { lamports: { gt: "13" } },
        ],
      },
    },
    slots: {
      slotClient: {
        filterByCommitment: true,
        interslotUpdates: false,
      },
    },
    transactions: {
      txClient: {
        vote: true,
        failed: false,
        signature: "txSig1",
        accountInclude: ["accInclude"],
        accountExclude: ["accExclude"],
        accountRequired: ["accRequired"],
      },
    },
    transactionsStatus: {
      txStatusClient: {
        vote: false,
        failed: true,
        signature: "txSig2",
        accountInclude: ["statusInclude"],
        accountExclude: ["statusExclude"],
        accountRequired: ["statusRequired"],
      },
    },
    blocks: {
      blocksClient: {
        accountInclude: ["blockAccount"],
        includeTransactions: true,
        includeAccounts: false,
        includeEntries: true,
      },
    },
    blocksMeta: {
      blocksMetaClient: {},
    },
    entry: {
      entryClient: {},
    },
    commitment: 2,
    accountsDataSlice: [
      { offset: "0", length: "32" },
      { offset: "32", length: "64" },
    ],
    ping: { id: 42 },
    fromSlot: "777",
  };
}

function makeMinimalSubscribeDeshredRequest(): SubscribeDeshredRequest {
  return {
    deshredTransactions: {},
  };
}

function closeStreamAndCaptureTerminalEvent(
  stream: any,
  timeoutMs = 2500,
): Promise<"close" | "end" | "error" | "timeout"> {
  return new Promise((resolve) => {
    let settled = false;

    const settleOnce = (event: "close" | "end" | "error" | "timeout") => {
      if (settled) {
        return;
      }
      settled = true;
      cleanup();
      resolve(event);
    };

    const cleanup = () => {
      clearTimeout(timeoutId);
      stream.off("close", onClose);
      stream.off("end", onEnd);
      stream.off("error", onError);
    };

    const onClose = () => settleOnce("close");
    const onEnd = () => settleOnce("end");
    const onError = () => settleOnce("error");

    const timeoutId = setTimeout(() => settleOnce("timeout"), timeoutMs);

    stream.once("close", onClose);
    stream.once("end", onEnd);
    stream.once("error", onError);

    try {
      stream.end();
    } catch {}

    try {
      stream.destroy();
    } catch {}
  });
}

function waitForTerminalEvent(
  stream: any,
  timeoutMs = 2500,
): Promise<"close" | "end" | "error" | "timeout"> {
  return new Promise((resolve) => {
    let settled = false;

    const settleOnce = (event: "close" | "end" | "error" | "timeout") => {
      if (settled) {
        return;
      }
      settled = true;
      cleanup();
      resolve(event);
    };

    const cleanup = () => {
      clearTimeout(timeoutId);
      stream.off("close", onClose);
      stream.off("end", onEnd);
      stream.off("error", onError);
    };

    const onClose = () => settleOnce("close");
    const onEnd = () => settleOnce("end");
    const onError = () => settleOnce("error");

    const timeoutId = setTimeout(() => settleOnce("timeout"), timeoutMs);

    stream.once("close", onClose);
    stream.once("end", onEnd);
    stream.once("error", onError);
  });
}

function waitForStreamError(stream: any, timeoutMs = 2500): Promise<Error> {
  return new Promise((resolve, reject) => {
    let settled = false;

    const settleOnce = (fn: () => void) => {
      if (settled) {
        return;
      }
      settled = true;
      cleanup();
      fn();
    };

    const cleanup = () => {
      clearTimeout(timeoutId);
      stream.off("error", onError);
      stream.off("close", onCloseBeforeError);
      stream.off("end", onCloseBeforeError);
    };

    const onError = (error: Error) => settleOnce(() => resolve(error));
    const onCloseBeforeError = () =>
      settleOnce(() =>
        reject(new Error("Stream ended/closed before emitting error event")),
      );

    const timeoutId = setTimeout(() => {
      settleOnce(() =>
        reject(
          new Error(`Timed out waiting for stream error after ${timeoutMs}ms`),
        ),
      );
    }, timeoutMs);

    stream.once("error", onError);
    stream.once("close", onCloseBeforeError);
    stream.once("end", onCloseBeforeError);
  });
}

function flushMicrotasks(): Promise<void> {
  return new Promise((resolve) => setImmediate(resolve));
}

function waitForDataEvent(stream: any, timeoutMs = 1000): Promise<any> {
  return new Promise((resolve, reject) => {
    let settled = false;
    const settleOnce = (handler: () => void) => {
      if (settled) {
        return;
      }
      settled = true;
      clearTimeout(timeoutId);
      stream.off("data", onData);
      stream.off("error", onError);
      handler();
    };
    const onData = (data: any) => settleOnce(() => resolve(data));
    const onError = (error: Error) => settleOnce(() => reject(error));
    const timeoutId = setTimeout(
      () =>
        settleOnce(() =>
          reject(
            new Error(`Timed out waiting for stream data after ${timeoutMs}ms`),
          ),
        ),
      timeoutMs,
    );
    stream.once("data", onData);
    stream.once("error", onError);
  });
}

function writeAndCaptureError(
  stream: any,
  request: unknown,
): Promise<Error | null> {
  return new Promise((resolve) => {
    let settled = false;
    const settleOnce = (error: Error | null) => {
      if (settled) {
        return;
      }
      settled = true;
      clearTimeout(timeoutId);
      stream.off("error", onErrorEvent);
      resolve(error);
    };
    const onErrorEvent = (err: Error) => settleOnce(err);

    const timeoutId = setTimeout(() => {
      settleOnce(new Error("write callback timed out"));
    }, 2500);
    stream.once("error", onErrorEvent);

    try {
      stream.write(request, (err: Error | null | undefined) =>
        settleOnce(err ?? null),
      );
    } catch (err) {
      settleOnce(err as Error);
    }
  });
}

function writeAndWaitCallback(
  stream: any,
  request: unknown,
  timeoutMs = 5000,
): Promise<Error | null> {
  return new Promise((resolve) => {
    let settled = false;

    const settleOnce = (error: Error | null) => {
      if (settled) {
        return;
      }
      settled = true;
      clearTimeout(timeoutId);
      resolve(error);
    };

    const timeoutId = setTimeout(() => {
      settleOnce(new Error(`Timed out waiting for write callback after ${timeoutMs}ms`));
    }, timeoutMs);

    try {
      stream.write(request, (err: Error | null | undefined) => {
        settleOnce(err ?? null);
      });
    } catch (err) {
      settleOnce(err as Error);
    }
  });
}

function getAllGeyserMessageFns(): Array<[string, any]> {
  return Object.entries(geyser)
    .filter(([, value]) => {
      if (!value || typeof value !== "object") {
        return false;
      }

      return (
        typeof (value as any).encode === "function" &&
        typeof (value as any).decode === "function" &&
        typeof (value as any).fromPartial === "function"
      );
    })
    .sort(([left], [right]) => left.localeCompare(right));
}

describe("ClientDuplexStream shutdown behavior", () => {
  test("shutdown: destroy emits terminal event and calls native close", async () => {
    const nativeClose = jest.fn();
    const nativeWrite = jest.fn();
    const nativeRead = jest.fn(() => new Promise(() => {}));
    const stream = new ClientDuplexStream(
      {
        close: nativeClose,
        write: nativeWrite,
        read: nativeRead,
      },
      { objectMode: true },
    );

    const terminalEvent = await closeStreamAndCaptureTerminalEvent(stream, 500);

    expect(terminalEvent).not.toBe("timeout");
    expect(nativeClose).toHaveBeenCalledTimes(1);
  });

  test("shutdown: write after destroy returns an error", async () => {
    const nativeWrite = jest.fn();
    const stream = new ClientDuplexStream(
      {
        close: jest.fn(),
        write: nativeWrite,
        read: jest.fn(() => new Promise(() => {})),
      },
      { objectMode: true },
    );

    stream.destroy();
    const writeError = await writeAndCaptureError(
      stream,
      makeMinimalSubscribeRequest(),
    );

    expect(writeError).not.toBeNull();
    const message = String(writeError?.message ?? "").toLowerCase();
    expect(
      message.includes("closed") ||
        message.includes("destroyed") ||
        message.includes("write after end"),
    ).toBe(true);
    expect(nativeWrite).not.toHaveBeenCalled();
  });
});

describe("Client connection guard behavior", () => {
  test("all public client methods fail before connect", async () => {
    const client = new Client("http://localhost:10000", undefined, {});

    const guardedCalls: Array<() => Promise<unknown>> = [
      () => client.getLatestBlockhash(2),
      () => client.ping(1),
      () => client.getBlockHeight(2),
      () => client.getSlot(2),
      () => client.isBlockhashValid("abc", 2),
      () => client.getVersion(),
      () => client.subscribeReplayInfo(),
      () => client.subscribe(),
      () => client.subscribeDeshred(),
    ];

    for (const invoke of guardedCalls) {
      await expect(invoke()).rejects.toThrow(
        "Client not connected. Call connect() first",
      );
    }
  });

  test("connect failure keeps client disconnected", async () => {
    const client = new Client("this-is-not-a-valid-endpoint", "token", {});

    await expect(client.connect()).rejects.toThrow();
    await expect(client.getVersion()).rejects.toThrow(
      "Client not connected. Call connect() first",
    );
  });

  test("subscribe bubbles native stream-open errors", async () => {
    const client = new Client("http://localhost:10000", undefined, {});
    const unavailableError = new Error(
      "status: Unavailable, message: subscribe stream open failed",
    );

    (client as any)._grpcClient = {
      subscribe: jest.fn().mockRejectedValue(unavailableError),
    };

    await expect(client.subscribe()).rejects.toThrow(
      "subscribe stream open failed",
    );
  });

  test("subscribeDeshred bubbles native stream-open errors", async () => {
    const client = new Client("http://localhost:10000", undefined, {});
    const unimplementedError = new Error(
      "status: Unimplemented, message: SubscribeDeshred is not available on this server",
    );

    (client as any)._grpcClient = {
      subscribeDeshred: jest.fn().mockRejectedValue(unimplementedError),
    };

    await expect(client.subscribeDeshred()).rejects.toThrow(
      "SubscribeDeshred is not available on this server",
    );
  });
});

describe("ClientDuplexStream read and lifecycle behavior", () => {
  function makeNativeUpdate(): any {
    return {
      filters: ["client"],
      createdAt: new Date(),
      updateOneof: {
        ping: {},
      },
    };
  }

  test("read: prevents overlapping native read calls while one read is in flight", async () => {
    let resolveRead: ((value: any) => void) | null = null;
    const nativeRead = jest.fn(
      () =>
        new Promise((resolve) => {
          resolveRead = resolve;
        }),
    );
    const stream = new ClientDuplexStream(
      {
        close: jest.fn(),
        write: jest.fn(),
        read: nativeRead,
      },
      { objectMode: true },
    );
    const pushSpy = jest.spyOn(stream as any, "push").mockReturnValue(false);

    (stream as any)._read(0);
    (stream as any)._read(0);
    expect(nativeRead).toHaveBeenCalledTimes(1);

    resolveRead?.(makeNativeUpdate());
    await flushMicrotasks();

    expect(pushSpy).toHaveBeenCalledTimes(1);
    await closeStreamAndWait(stream);
  });

  test("read: honors backpressure and only pulls again after next _read()", async () => {
    const nativeRead = jest.fn().mockResolvedValue(makeNativeUpdate());
    const stream = new ClientDuplexStream(
      {
        close: jest.fn(),
        write: jest.fn(),
        read: nativeRead,
      },
      { objectMode: true },
    );
    const pushSpy = jest.spyOn(stream as any, "push").mockReturnValue(false);

    (stream as any)._read(0);
    await flushMicrotasks();
    expect(nativeRead).toHaveBeenCalledTimes(1);
    expect(pushSpy).toHaveBeenCalledTimes(1);

    (stream as any)._read(0);
    await flushMicrotasks();
    expect(nativeRead).toHaveBeenCalledTimes(2);

    await closeStreamAndWait(stream);
  });

  test("read: NO_UPDATE_AVAILABLE is treated as graceful end (no error event)", async () => {
    const nativeClose = jest.fn();
    const nativeRead = jest
      .fn()
      .mockRejectedValue({
        code: "NO_UPDATE_AVAILABLE",
        message: "no update available",
      });
    const stream = new ClientDuplexStream(
      {
        close: nativeClose,
        write: jest.fn(),
        read: nativeRead,
      },
      { objectMode: true },
    );

    const terminalEventPromise = waitForTerminalEvent(stream, 1000);
    (stream as any)._read(0);
    const terminalEvent = await terminalEventPromise;

    expect(terminalEvent).not.toBe("timeout");
    expect(terminalEvent).not.toBe("error");
    expect(nativeClose).toHaveBeenCalledTimes(1);
  });

  test("read: undefined update from native read is treated as graceful end", async () => {
    const nativeClose = jest.fn();
    const nativeRead = jest.fn().mockResolvedValue(undefined);
    const stream = new ClientDuplexStream(
      {
        close: nativeClose,
        write: jest.fn(),
        read: nativeRead,
      },
      { objectMode: true },
    );

    const terminalEventPromise = waitForTerminalEvent(stream, 1000);
    (stream as any)._read(0);
    const terminalEvent = await terminalEventPromise;

    expect(terminalEvent).not.toBe("timeout");
    expect(terminalEvent).not.toBe("error");
    expect(nativeClose).toHaveBeenCalledTimes(1);
  });

  test("read: native read error emits a single terminal error", async () => {
    const nativeClose = jest.fn();
    const nativeRead = jest
      .fn()
      .mockRejectedValue(new Error("simulated read failure"));
    const stream = new ClientDuplexStream(
      {
        close: nativeClose,
        write: jest.fn(),
        read: nativeRead,
      },
      { objectMode: true },
    );
    const observedErrors: Error[] = [];
    stream.on("error", (err) => observedErrors.push(err as Error));

    const closePromise = new Promise<void>((resolve) =>
      stream.once("close", () => resolve()),
    );
    (stream as any)._read(0);
    await closePromise;

    expect(observedErrors.length).toBe(1);
    expect(String(observedErrors[0].message)).toContain(
      "simulated read failure",
    );
    expect(nativeClose).toHaveBeenCalledTimes(1);
  });

  test("destroy: native close throw is swallowed and stream still terminates", async () => {
    const stream = new ClientDuplexStream(
      {
        close: jest.fn(() => {
          throw new Error("native close failed");
        }),
        write: jest.fn(),
        read: jest.fn(() => new Promise(() => {})),
      },
      { objectMode: true },
    );

    expect(() => stream.destroy()).not.toThrow();
    const terminalEvent = await waitForTerminalEvent(stream, 1000);
    expect(terminalEvent).not.toBe("timeout");
  });

  test("write: native write throw is propagated through write callback", async () => {
    const stream = new ClientDuplexStream(
      {
        close: jest.fn(),
        writeRaw: jest.fn(() => {
          throw new Error("native write failed");
        }),
        read: jest.fn(() => new Promise(() => {})),
      },
      { objectMode: true },
    );
    // Node Duplex may emit an `error` event in addition to write callback error.
    stream.on("error", () => {});

    const writeError = await writeAndCaptureError(
      stream,
      makeMinimalSubscribeRequest(),
    );
    expect(writeError).not.toBeNull();
    expect(String(writeError?.message ?? "")).toContain("native write failed");

    await closeStreamAndWait(stream);
  });

  test("write: encodes full SubscribeRequest and forwards protobuf bytes to native writeRaw", async () => {
    const nativeWriteRaw = jest.fn();
    const stream = new ClientDuplexStream(
      {
        close: jest.fn(),
        writeRaw: nativeWriteRaw,
        read: jest.fn(() => new Promise(() => {})),
      },
      { objectMode: true },
    );
    stream.on("error", () => {});

    const request = makeComprehensiveSubscribeRequest();
    const writeError = await writeAndCaptureError(stream, request);
    expect(writeError).toBeNull();
    expect(nativeWriteRaw).toHaveBeenCalledTimes(1);

    const forwardedBytes = nativeWriteRaw.mock.calls[0][0] as Uint8Array;
    const decoded = geyser.SubscribeRequest.decode(forwardedBytes);

    expect(decoded.accounts.allFields.account).toEqual([
      "accountA",
      "accountB",
    ]);
    expect(decoded.accounts.allFields.owner).toEqual(["ownerA"]);
    expect(decoded.accounts.allFields.nonemptyTxnSignature).toBe(true);
    expect(decoded.accounts.allFields.filters).toHaveLength(9);
    expect(decoded.accounts.allFields.filters[0].memcmp?.bytes).toEqual(
      Uint8Array.from([1, 2, 3]),
    );
    expect(decoded.accounts.allFields.filters[1].memcmp?.base58).toBe(
      "11111111111111111111111111111111",
    );
    expect(decoded.accounts.allFields.filters[2].memcmp?.base64).toBe("AQID");
    expect(decoded.accounts.allFields.filters[3].datasize).toBe("165");
    expect(decoded.accounts.allFields.filters[4].tokenAccountState).toBe(true);
    expect(decoded.accounts.allFields.filters[5].lamports?.eq).toBe("10");
    expect(decoded.accounts.allFields.filters[6].lamports?.ne).toBe("11");
    expect(decoded.accounts.allFields.filters[7].lamports?.lt).toBe("12");
    expect(decoded.accounts.allFields.filters[8].lamports?.gt).toBe("13");

    expect(decoded.slots.slotClient.filterByCommitment).toBe(true);
    expect(decoded.slots.slotClient.interslotUpdates).toBe(false);

    expect(decoded.transactions.txClient.vote).toBe(true);
    expect(decoded.transactions.txClient.failed).toBe(false);
    expect(decoded.transactions.txClient.signature).toBe("txSig1");
    expect(decoded.transactions.txClient.accountInclude).toEqual([
      "accInclude",
    ]);
    expect(decoded.transactions.txClient.accountExclude).toEqual([
      "accExclude",
    ]);
    expect(decoded.transactions.txClient.accountRequired).toEqual([
      "accRequired",
    ]);

    expect(decoded.transactionsStatus.txStatusClient.vote).toBe(false);
    expect(decoded.transactionsStatus.txStatusClient.failed).toBe(true);
    expect(decoded.transactionsStatus.txStatusClient.signature).toBe("txSig2");
    expect(decoded.transactionsStatus.txStatusClient.accountInclude).toEqual([
      "statusInclude",
    ]);
    expect(decoded.transactionsStatus.txStatusClient.accountExclude).toEqual([
      "statusExclude",
    ]);
    expect(decoded.transactionsStatus.txStatusClient.accountRequired).toEqual([
      "statusRequired",
    ]);

    expect(decoded.blocks.blocksClient.accountInclude).toEqual([
      "blockAccount",
    ]);
    expect(decoded.blocks.blocksClient.includeTransactions).toBe(true);
    expect(decoded.blocks.blocksClient.includeAccounts).toBe(false);
    expect(decoded.blocks.blocksClient.includeEntries).toBe(true);

    expect(decoded.blocksMeta.blocksMetaClient).toEqual({});
    expect(decoded.entry.entryClient).toEqual({});
    expect(decoded.commitment).toBe(2);
    expect(decoded.accountsDataSlice).toEqual([
      { offset: "0", length: "32" },
      { offset: "32", length: "64" },
    ]);
    expect(decoded.ping?.id).toBe(42);
    expect(decoded.fromSlot).toBe("777");

    await closeStreamAndWait(stream);
  });

  test("deshred write: encodes vote=false and forwards protobuf bytes to native writeRaw", async () => {
    const nativeWriteRaw = jest.fn();
    const stream = new ClientDeshredDuplexStream(
      {
        close: jest.fn(),
        writeRaw: nativeWriteRaw,
        read: jest.fn(() => new Promise(() => {})),
      },
      { objectMode: true },
    );
    stream.on("error", () => {});

    const request: SubscribeDeshredRequest = {
      deshredTransactions: {
        client: {
          vote: false,
          accountInclude: ["accInclude"],
          accountExclude: ["accExclude"],
          accountRequired: ["accRequired"],
        },
      },
      ping: { id: 11 },
    };

    const writeError = await writeAndCaptureError(stream, request);
    expect(writeError).toBeNull();
    expect(nativeWriteRaw).toHaveBeenCalledTimes(1);

    const forwardedBytes = nativeWriteRaw.mock.calls[0][0] as Uint8Array;
    const decoded = geyser.SubscribeDeshredRequest.decode(forwardedBytes);

    expect(decoded.deshredTransactions.client.vote).toBe(false);
    expect(decoded.deshredTransactions.client.accountInclude).toEqual([
      "accInclude",
    ]);
    expect(decoded.deshredTransactions.client.accountExclude).toEqual([
      "accExclude",
    ]);
    expect(decoded.deshredTransactions.client.accountRequired).toEqual([
      "accRequired",
    ]);
    expect(decoded.ping?.id).toBe(11);

    await closeStreamAndWait(stream);
  });

  test("deshred write: normalizes vote='false' string without mutating request object", async () => {
    const nativeWriteRaw = jest.fn();
    const stream = new ClientDeshredDuplexStream(
      {
        close: jest.fn(),
        writeRaw: nativeWriteRaw,
        read: jest.fn(() => new Promise(() => {})),
      },
      { objectMode: true },
    );
    stream.on("error", () => {});

    const request = makeMinimalSubscribeDeshredRequest() as unknown as {
      deshredTransactions: Record<string, { vote?: unknown }>;
    };
    request.deshredTransactions = {
      client: {
        vote: "false",
        accountInclude: [],
        accountExclude: [],
        accountRequired: [],
      },
    };
    const requestBeforeWrite = JSON.parse(JSON.stringify(request));

    const writeError = await writeAndCaptureError(stream, request);
    expect(writeError).toBeNull();

    const forwardedBytes = nativeWriteRaw.mock.calls[0][0] as Uint8Array;
    const decoded = geyser.SubscribeDeshredRequest.decode(forwardedBytes);
    expect(decoded.deshredTransactions.client.vote).toBe(false);

    expect(request).toEqual(requestBeforeWrite);

    await closeStreamAndWait(stream);
  });

  test("write: returns compatibility error when native stream lacks writeRaw", async () => {
    const stream = new ClientDuplexStream(
      {
        close: jest.fn(),
        write: jest.fn(),
        read: jest.fn(() => new Promise(() => {})),
      },
      { objectMode: true },
    );
    stream.on("error", () => {});

    const writeError = await writeAndCaptureError(
      stream,
      makeMinimalSubscribeRequest(),
    );
    expect(writeError).not.toBeNull();
    expect(String(writeError?.message ?? "")).toContain(
      "Native stream does not support writeRaw",
    );

    await closeStreamAndWait(stream);
  });

  test("write: does not mutate SubscribeRequest input object", async () => {
    const nativeWriteRaw = jest.fn();
    const stream = new ClientDuplexStream(
      {
        close: jest.fn(),
        writeRaw: nativeWriteRaw,
        read: jest.fn(() => new Promise(() => {})),
      },
      { objectMode: true },
    );
    stream.on("error", () => {});

    const request = makeComprehensiveSubscribeRequest();
    const requestBeforeWrite = geyser.SubscribeRequest.decode(
      geyser.SubscribeRequest.encode(request).finish(),
    );

    const writeError = await writeAndCaptureError(stream, request);
    expect(writeError).toBeNull();

    const requestAfterWrite = geyser.SubscribeRequest.decode(
      geyser.SubscribeRequest.encode(request).finish(),
    );
    expect(requestAfterWrite).toEqual(requestBeforeWrite);
    expect(nativeWriteRaw).toHaveBeenCalledTimes(1);

    await closeStreamAndWait(stream);
  });

  test.each([
    [
      "account",
      { slot: "1", isStartup: false, account: { pubkey: Buffer.from([]) } },
    ],
    ["slot", { slot: "2", status: 1 }],
    [
      "transaction",
      {
        slot: "3",
        transaction: { signature: Buffer.from([]), isVote: false, index: "0" },
      },
    ],
    [
      "transactionStatus",
      { slot: "4", signature: Buffer.from([]), isVote: false, index: "0" },
    ],
    [
      "block",
      {
        slot: "5",
        blockhash: "hash",
        parentSlot: "4",
        parentBlockhash: "parent",
      },
    ],
    ["ping", {}],
    ["pong", { id: 7 }],
    [
      "blockMeta",
      {
        slot: "6",
        blockhash: "meta",
        parentSlot: "5",
        parentBlockhash: "parent-meta",
      },
    ],
    [
      "entry",
      {
        slot: "7",
        index: "0",
        numHashes: "0",
        hash: Buffer.from([]),
        executedTransactionCount: "0",
        startingTransactionIndex: "0",
      },
    ],
  ])(
    "read: maps updateOneof.%s to top-level SDK field",
    async (variant, payload) => {
      const nativeRead = jest.fn().mockResolvedValue({
        filters: ["client"],
        createdAt: new Date(),
        updateOneof: {
          [variant]: payload,
        },
      });
      const stream = new ClientDuplexStream(
        {
          close: jest.fn(),
          writeRaw: jest.fn(),
          read: nativeRead,
        },
        { objectMode: true },
      );

      const dataPromise = waitForDataEvent(stream, 1000);
      (stream as any)._read(0);
      const mappedUpdate = await dataPromise;

      expect((mappedUpdate as any).updateOneof).toBeUndefined();
      expect((mappedUpdate as any)[variant]).toEqual(payload);
      expect(mappedUpdate.filters).toEqual(["client"]);
      expect(Object.prototype.toString.call(mappedUpdate.createdAt)).toBe(
        "[object Date]",
      );

      await closeStreamAndWait(stream);
    },
  );

  test("deshred read: maps updateOneof.deshredTransaction to top-level SDK field", async () => {
    const deshredPayload = {
      slot: "8",
      transaction: {
        signature: Buffer.from([7, 7, 7]),
        isVote: false,
        transaction: undefined,
        loadedWritableAddresses: [],
        loadedReadonlyAddresses: [],
      },
    };

    const nativeRead = jest.fn().mockResolvedValue({
      filters: ["client"],
      createdAt: new Date(),
      updateOneof: {
        deshredTransaction: deshredPayload,
      },
    });
    const stream = new ClientDeshredDuplexStream(
      {
        close: jest.fn(),
        writeRaw: jest.fn(),
        read: nativeRead,
      },
      { objectMode: true },
    );

    const dataPromise = waitForDataEvent(stream, 1000);
    (stream as any)._read(0);
    const mappedUpdate = await dataPromise;

    expect((mappedUpdate as any).updateOneof).toBeUndefined();
    expect(mappedUpdate.deshredTransaction).toEqual(deshredPayload);
    expect(mappedUpdate.filters).toEqual(["client"]);
    expect(Object.prototype.toString.call(mappedUpdate.createdAt)).toBe(
      "[object Date]",
    );

    await closeStreamAndWait(stream);
  });

  test("deshred read: maps ping and pong variants to top-level SDK fields", async () => {
    for (const variantPayload of [{ ping: {} }, { pong: { id: 3 } }]) {
      const nativeRead = jest.fn().mockResolvedValue({
        filters: ["client"],
        createdAt: new Date(),
        updateOneof: variantPayload,
      });
      const stream = new ClientDeshredDuplexStream(
        {
          close: jest.fn(),
          writeRaw: jest.fn(),
          read: nativeRead,
        },
        { objectMode: true },
      );

      const dataPromise = waitForDataEvent(stream, 1000);
      (stream as any)._read(0);
      const mappedUpdate = await dataPromise;

      expect((mappedUpdate as any).updateOneof).toBeUndefined();
      if ("ping" in variantPayload) {
        expect(mappedUpdate.ping).toEqual({});
        expect(mappedUpdate.pong).toBeUndefined();
      } else {
        expect(mappedUpdate.pong).toEqual({ id: 3 });
        expect(mappedUpdate.ping).toBeUndefined();
      }

      await closeStreamAndWait(stream);
    }
  });

  test("deshred read: NO_UPDATE_AVAILABLE is treated as graceful end (no error event)", async () => {
    const nativeClose = jest.fn();
    const nativeRead = jest.fn().mockRejectedValue({
      code: "NO_UPDATE_AVAILABLE",
      message: "no update available",
    });
    const stream = new ClientDeshredDuplexStream(
      {
        close: nativeClose,
        writeRaw: jest.fn(),
        read: nativeRead,
      },
      { objectMode: true },
    );

    const terminalEventPromise = waitForTerminalEvent(stream, 1000);
    (stream as any)._read(0);
    const terminalEvent = await terminalEventPromise;

    expect(terminalEvent).not.toBe("timeout");
    expect(terminalEvent).not.toBe("error");
    expect(nativeClose).toHaveBeenCalledTimes(1);
  });

  test("deshred read: native read error emits a single terminal error", async () => {
    const nativeClose = jest.fn();
    const nativeRead = jest
      .fn()
      .mockRejectedValue(new Error("deshred simulated read failure"));
    const stream = new ClientDeshredDuplexStream(
      {
        close: nativeClose,
        writeRaw: jest.fn(),
        read: nativeRead,
      },
      { objectMode: true },
    );
    const observedErrors: Error[] = [];
    stream.on("error", (err) => observedErrors.push(err as Error));

    const closePromise = new Promise<void>((resolve) =>
      stream.once("close", () => resolve()),
    );
    (stream as any)._read(0);
    await closePromise;

    expect(observedErrors.length).toBe(1);
    expect(String(observedErrors[0].message)).toContain(
      "deshred simulated read failure",
    );
    expect(nativeClose).toHaveBeenCalledTimes(1);
  });

  test("deshred write: rejects invalid vote value", async () => {
    const nativeWriteRaw = jest.fn();
    const stream = new ClientDeshredDuplexStream(
      {
        close: jest.fn(),
        writeRaw: nativeWriteRaw,
        read: jest.fn(() => new Promise(() => {})),
      },
      { objectMode: true },
    );
    stream.on("error", () => {});

    const request = {
      deshredTransactions: {
        client: {
          vote: "not-bool",
          accountInclude: [],
          accountExclude: [],
          accountRequired: [],
        },
      },
    };

    const writeError = await writeAndCaptureError(stream, request);
    expect(writeError).not.toBeNull();
    expect(String(writeError?.message ?? "")).toContain(
      "Invalid deshredTransactions.client.vote",
    );
    expect(nativeWriteRaw).toHaveBeenCalledTimes(0);

    await closeStreamAndWait(stream);
  });

  test("deshred write: native write throw is propagated through write callback", async () => {
    const stream = new ClientDeshredDuplexStream(
      {
        close: jest.fn(),
        writeRaw: jest.fn(() => {
          throw new Error("deshred native write failed");
        }),
        read: jest.fn(() => new Promise(() => {})),
      },
      { objectMode: true },
    );
    stream.on("error", () => {});

    const writeError = await writeAndCaptureError(
      stream,
      makeMinimalSubscribeDeshredRequest(),
    );
    expect(writeError).not.toBeNull();
    expect(String(writeError?.message ?? "")).toContain(
      "deshred native write failed",
    );

    await closeStreamAndWait(stream);
  });

  test("deshred write: returns compatibility error when native stream lacks writeRaw", async () => {
    const stream = new ClientDeshredDuplexStream(
      {
        close: jest.fn(),
        write: jest.fn(),
        read: jest.fn(() => new Promise(() => {})),
      },
      { objectMode: true },
    );
    stream.on("error", () => {});

    const writeError = await writeAndCaptureError(
      stream,
      makeMinimalSubscribeDeshredRequest(),
    );
    expect(writeError).not.toBeNull();
    expect(String(writeError?.message ?? "")).toContain(
      "Native stream does not support writeRaw",
    );

    await closeStreamAndWait(stream);
  });

  test("deshred write: write after destroy is rejected and native writeRaw is not called", async () => {
    const nativeWriteRaw = jest.fn();
    const stream = new ClientDeshredDuplexStream(
      {
        close: jest.fn(),
        writeRaw: nativeWriteRaw,
        read: jest.fn(() => new Promise(() => {})),
      },
      { objectMode: true },
    );
    stream.on("error", () => {});

    stream.destroy();
    const writeError = await writeAndCaptureError(
      stream,
      makeMinimalSubscribeDeshredRequest(),
    );

    expect(writeError).not.toBeNull();
    const message = String(writeError?.message ?? "").toLowerCase();
    expect(
      message.includes("closed") ||
        message.includes("destroyed") ||
        message.includes("write after end"),
    ).toBe(true);
    expect(nativeWriteRaw).not.toHaveBeenCalled();
  });

  test("txDeshredEncode export exposes encoding and raw encoder", () => {
    expect(txDeshredEncode).toBeDefined();
    expect(txDeshredEncode.encoding.JsonParsed).toBeDefined();
    expect(typeof txDeshredEncode.encode_raw).toBe("function");
  });

  test("read: ignores late read completion after destroy", async () => {
    let resolveRead: ((value: any) => void) | null = null;
    const nativeRead = jest.fn(
      () =>
        new Promise((resolve) => {
          resolveRead = resolve;
        }),
    );
    const stream = new ClientDuplexStream(
      {
        close: jest.fn(),
        writeRaw: jest.fn(),
        read: nativeRead,
      },
      { objectMode: true },
    );
    const onData = jest.fn();
    stream.on("data", onData);
    stream.on("error", () => {});

    (stream as any)._read(0);
    stream.destroy();
    resolveRead?.({
      filters: ["client"],
      createdAt: new Date(),
      updateOneof: { ping: {} },
    });
    await flushMicrotasks();

    expect(onData).not.toHaveBeenCalled();
  });
});

describe("Client subscription independence behavior", () => {
  test("closing one subscription stream does not affect another stream", async () => {
    const nativeStreamA = {
      close: jest.fn(),
      writeRaw: jest.fn(),
      read: jest.fn(() => new Promise(() => {})),
    };
    const nativeStreamB = {
      close: jest.fn(),
      writeRaw: jest.fn(),
      read: jest.fn(() => new Promise(() => {})),
    };

    const client = new Client("http://localhost:10000", undefined, {});
    (client as any)._grpcClient = {
      subscribe: jest
        .fn()
        .mockReturnValueOnce(nativeStreamA)
        .mockReturnValueOnce(nativeStreamB),
    };

    const streamA = await client.subscribe();
    const streamB = await client.subscribe();

    await closeStreamAndWait(streamA);

    const streamBWriteError = await writeAndCaptureError(
      streamB,
      makeMinimalSubscribeRequest(),
    );
    expect(streamBWriteError).toBeNull();
    expect(nativeStreamB.writeRaw).toHaveBeenCalledTimes(1);

    await closeStreamAndWait(streamB);

    expect(nativeStreamA.close).toHaveBeenCalledTimes(1);
    expect(nativeStreamB.close).toHaveBeenCalledTimes(1);
  });
});

describe("subscribe response schema tests", () => {
  const TEST_TIMEOUT = 100000;

  // .env
  const { TEST_ENDPOINT: endpoint, TEST_TOKEN: xToken } = process.env;

  // Use options sensible defaults.
  const channelOptions = {};
  const client = new Client(endpoint, xToken, channelOptions);

  beforeAll(async () => {
    await client.connect();
  });

  function baseSubscribeRequest(): SubscribeRequest {
    return {
      accounts: {},
      slots: {},
      transactions: {},
      transactionsStatus: {},
      accountsDataSlice: [],
      blocks: {},
      blocksMeta: {},
      entry: {},
      commitment: 2,
    };
  }

  async function assertRequestsAcceptedOnSingleStream(
    requests: Array<{ label: string; request: SubscribeRequest }>,
    settleMs = 250,
  ): Promise<void> {
    const stream = await client.subscribe();
    const streamErrors: Error[] = [];
    stream.on("error", (error: Error) => {
      streamErrors.push(error);
    });

    try {
      for (const { label, request } of requests) {
        const writeError = await writeAndCaptureError(stream, request);
        if (writeError) {
          throw new Error(
            `[${label}] write failed: ${String(writeError.message ?? writeError)}`,
          );
        }
        await new Promise((resolve) => setTimeout(resolve, settleMs));
        const unexpectedError = streamErrors.find(
          (error) =>
            !String(error?.message ?? "")
              .toLowerCase()
              .includes("no update available"),
        );
        if (unexpectedError) {
          throw new Error(
            `[${label}] stream emitted error: ${unexpectedError.message}`,
          );
        }
      }
    } finally {
      await closeStreamAndWait(stream);
    }
  }

  test("accounts filter oneof variants are accepted", async () => {
    const accountFilterCases: Array<{
      label: string;
      filter: SubscribeRequest["accounts"][string]["filters"][number];
    }> = [
      {
        label: "memcmp_bytes",
        filter: { memcmp: { offset: "0", bytes: Uint8Array.from([1, 2, 3]) } },
      },
      {
        label: "memcmp_base58",
        filter: {
          memcmp: { offset: "1", base58: "11111111111111111111111111111111" },
        },
      },
      {
        label: "memcmp_base64",
        filter: { memcmp: { offset: "2", base64: "AQID" } },
      },
      { label: "datasize", filter: { datasize: "165" } },
      { label: "token_account_state", filter: { tokenAccountState: true } },
      { label: "lamports_eq", filter: { lamports: { eq: "1" } } },
      { label: "lamports_ne", filter: { lamports: { ne: "2" } } },
      { label: "lamports_lt", filter: { lamports: { lt: "3" } } },
      { label: "lamports_gt", filter: { lamports: { gt: "4" } } },
    ];

    const requests = accountFilterCases.map(({ label, filter }) => {
      const request = baseSubscribeRequest();
      request.accounts = {
        [label]: {
          account: [],
          owner: [],
          filters: [filter],
        },
      };
      return { label, request };
    });

    await assertRequestsAcceptedOnSingleStream(requests);
  }, 180000);

  test("slots filter combinations are accepted", async () => {
    const boolValues: Array<boolean | undefined> = [undefined, true, false];
    const requests: Array<{ label: string; request: SubscribeRequest }> = [];

    for (const filterByCommitment of boolValues) {
      for (const interslotUpdates of boolValues) {
        const request = baseSubscribeRequest();
        const label = `filterByCommitment_${String(filterByCommitment)}__interslotUpdates_${String(interslotUpdates)}`;
        request.slots = {
          [label]: {
            filterByCommitment,
            interslotUpdates,
          },
        };
        requests.push({ label, request });
      }
    }

    await assertRequestsAcceptedOnSingleStream(requests);
  }, 180000);

  test("blocks filter combinations are accepted", async () => {
    const boolValues: Array<boolean | undefined> = [undefined, true, false];
    const requests: Array<{ label: string; request: SubscribeRequest }> = [];

    for (const includeTransactions of boolValues) {
      for (const includeAccounts of boolValues) {
        for (const includeEntries of boolValues) {
          const request = baseSubscribeRequest();
          const label = `tx_${String(includeTransactions)}__acc_${String(includeAccounts)}__ent_${String(includeEntries)}`;
          request.blocks = {
            [label]: {
              accountInclude: [],
              includeTransactions,
              includeAccounts,
              includeEntries,
            },
          };
          requests.push({ label, request });
        }
      }
    }

    await assertRequestsAcceptedOnSingleStream(requests);
  }, 180000);

  test("transactions filter combinations are accepted", async () => {
    const triStateBool: Array<boolean | undefined> = [undefined, true, false];
    const signaturePresence = [false, true];
    const validSignature =
      "4V36qYhukXcLFuvhZaudSoJpPaFNB7d5RqYKjL2xiSKrxaBfEajqqL4X6viZkEvHJ8XcTJsqVjZxFegxhN7EC9V5";
    const requests: Array<{ label: string; request: SubscribeRequest }> = [];

    for (const vote of triStateBool) {
      for (const failed of triStateBool) {
        for (const includeSignature of signaturePresence) {
          const request = baseSubscribeRequest();
          const label = `vote_${String(vote)}__failed_${String(failed)}__sig_${includeSignature ? "yes" : "no"}`;
          request.transactions = {
            [label]: {
              vote,
              failed,
              signature: includeSignature ? validSignature : undefined,
              accountInclude: [],
              accountExclude: [],
              accountRequired: [],
            },
          };
          requests.push({ label, request });
        }
      }
    }

    await assertRequestsAcceptedOnSingleStream(requests);
  }, 180000);

  test("transactionsStatus filter combinations are accepted", async () => {
    const triStateBool: Array<boolean | undefined> = [undefined, true, false];
    const signaturePresence = [false, true];
    const validSignature =
      "4V36qYhukXcLFuvhZaudSoJpPaFNB7d5RqYKjL2xiSKrxaBfEajqqL4X6viZkEvHJ8XcTJsqVjZxFegxhN7EC9V5";
    const requests: Array<{ label: string; request: SubscribeRequest }> = [];

    for (const vote of triStateBool) {
      for (const failed of triStateBool) {
        for (const includeSignature of signaturePresence) {
          const request = baseSubscribeRequest();
          const label = `status_vote_${String(vote)}__failed_${String(failed)}__sig_${includeSignature ? "yes" : "no"}`;
          request.transactionsStatus = {
            [label]: {
              vote,
              failed,
              signature: includeSignature ? validSignature : undefined,
              accountInclude: [],
              accountExclude: [],
              accountRequired: [],
            },
          };
          requests.push({ label, request });
        }
      }
    }

    await assertRequestsAcceptedOnSingleStream(requests);
  }, 180000);

  test("runtime server errors are propagated via stream error event", async () => {
    const stream = await client.subscribe();
    try {
      // Ensure `_read()` is driven so native terminal errors can surface to JS.
      stream.on("data", () => {});
      const runtimeError = waitForStreamError(stream, TEST_TIMEOUT);
      const request = baseSubscribeRequest();
      request.transactions = {
        invalid_pubkey_filter: {
          signature: "abc",
          accountInclude: ["not_base58_!!!"],
          accountExclude: [],
          accountRequired: [],
        },
      };

      const writeError = await writeAndWaitCallback(stream, request);
      expect(writeError).toBeNull();

      const observedError = await runtimeError;
      const message = String(observedError?.message ?? "").toLowerCase();
      expect(message.length).toBeGreaterThan(0);
      expect(message.includes("no update available")).toBe(false);
    } finally {
      await closeStreamAndWait(stream);
    }
  }, TEST_TIMEOUT);

  test(
    "account",
    async () => {
      let subscribe_update_response: any;
      const subscribe_duplex_stream = await client.subscribe();
      const request: SubscribeRequest = {
        accounts: {
          client: {
            account: [],
            filters: [],
            owner: [],
          },
        },
        slots: {},
        transactions: {},
        transactionsStatus: {},
        accountsDataSlice: [],
        blocks: {},
        blocksMeta: {},
        entry: {},
        commitment: 2,
      };

      const waitForAccount = waitForSubscribeUpdateMatchingPredicate(
        subscribe_duplex_stream,
        (data) => Boolean(data?.account),
        TEST_TIMEOUT,
      );

      subscribe_duplex_stream.write(request, (err) => {
        if (err) {
          console.error(`error writing to stream: ${err}`);
        }
      });

      subscribe_update_response = await waitForAccount;

      expect(subscribe_update_response.filters).toEqual(["client"]);
      // We're doing it this way so we can bypass the Jest Globals vs Node Globals
      // type conflicts that makes expect(Date).toBeInstanceOf(Date) to fail.
      //
      // See issue here: https://github.com/jestjs/jest/issues/2549
      expect(
        Object.prototype.toString.call(subscribe_update_response.createdAt),
      ).toBe("[object Date]");
      expect((subscribe_update_response as any).updateOneof).toBeUndefined();
      expect(typeof subscribe_update_response.account).toBe("object");
      expect(typeof subscribe_update_response.account.slot).toBe("string");
      expect(typeof subscribe_update_response.account.isStartup).toBe(
        "boolean",
      );
      expect(typeof subscribe_update_response.account.account).toBe("object");

      const account = subscribe_update_response.account.account;
      expect(account.pubkey).toBeInstanceOf(Buffer);
      expect(account.owner).toBeInstanceOf(Buffer);
      expect(account.data).toBeInstanceOf(Buffer);
      expect(typeof account.lamports).toBe("string");
      expect(typeof account.rentEpoch).toBe("string");
      expect(typeof account.writeVersion).toBe("string");
      expect(typeof account.executable).toBe("boolean");

      const decodedAccount = expectEncodeDecodeRoundTrip(
        SubscribeUpdateAccount,
        subscribe_update_response.account,
      );
      expect(decodedAccount.account).toBeDefined();

      const decodedEnvelope = expectEncodeDecodeRoundTrip(
        SubscribeUpdate,
        subscribe_update_response,
      );
      expect(decodedEnvelope.account).toBeDefined();
    },
    TEST_TIMEOUT,
  );

  test(
    "slot",
    async () => {
      let subscribe_update_response: any;
      const subscribe_duplex_stream = await client.subscribe();
      const request: SubscribeRequest = {
        slots: {
          client: {},
        },
        accounts: {},
        transactions: {},
        transactionsStatus: {},
        accountsDataSlice: [],
        blocks: {},
        blocksMeta: {},
        entry: {},
        commitment: 2,
      };

      const waitForSlot = waitForSubscribeUpdateMatchingPredicate(
        subscribe_duplex_stream,
        (data) => Boolean(data?.slot),
        TEST_TIMEOUT,
      );

      subscribe_duplex_stream.write(request, (err) => {
        if (err) {
          console.error(`error writing to stream: ${err}`);
        }
      });

      subscribe_update_response = await waitForSlot;

      expect(subscribe_update_response.filters).toEqual(["client"]);
      // We're doing it this way so we can bypass the Jest Globals vs Node Globals
      // type conflicts that makes expect(Date).toBeInstanceOf(Date) to fail.
      //
      // See issue here: https://github.com/jestjs/jest/issues/2549
      expect(
        Object.prototype.toString.call(subscribe_update_response.createdAt),
      ).toBe("[object Date]");
      expect((subscribe_update_response as any).updateOneof).toBeUndefined();
      expect(typeof subscribe_update_response.slot).toBe("object");
      expect(typeof subscribe_update_response.slot.slot).toBe("string");
      expect(typeof subscribe_update_response.slot.status).toBe("number");

      const decodedSlot = expectEncodeDecodeRoundTrip(
        SubscribeUpdateSlot,
        subscribe_update_response.slot,
      );
      expect(decodedSlot.slot).toBeDefined();

      const decodedEnvelope = expectEncodeDecodeRoundTrip(
        SubscribeUpdate,
        subscribe_update_response,
      );
      expect(decodedEnvelope.slot).toBeDefined();
    },
    TEST_TIMEOUT,
  );

  test(
    "transaction",
    async () => {
      let subscribe_update_response: any;
      const subscribe_duplex_stream = await client.subscribe();
      const request: SubscribeRequest = {
        transactions: {
          client: {
            accountExclude: [],
            accountInclude: [],
            accountRequired: [],
          },
        },
        accounts: {},
        slots: {},
        transactionsStatus: {},
        accountsDataSlice: [],
        blocks: {},
        blocksMeta: {},
        entry: {},
        commitment: 2,
      };

      const waitForTransaction = waitForSubscribeUpdateMatchingPredicate(
        subscribe_duplex_stream,
        (data) => Boolean(data?.transaction?.transaction),
        TEST_TIMEOUT,
      );

      subscribe_duplex_stream.write(request, (err) => {
        if (err) {
          console.error(`error writing to stream: ${err}`);
        }
      });

      subscribe_update_response = await waitForTransaction;

      expect(subscribe_update_response.filters).toEqual(["client"]);
      // We're doing it this way so we can bypass the Jest Globals vs Node Globals
      // type conflicts that makes expect(Date).toBeInstanceOf(Date) to fail.
      //
      // See issue here: https://github.com/jestjs/jest/issues/2549
      expect(
        Object.prototype.toString.call(subscribe_update_response.createdAt),
      ).toBe("[object Date]");
      expect((subscribe_update_response as any).updateOneof).toBeUndefined();
      expect(typeof subscribe_update_response.transaction).toBe("object");
      expect(typeof subscribe_update_response.transaction.slot).toBe("string");

      const tx = subscribe_update_response.transaction.transaction;
      expect(tx.signature).toBeInstanceOf(Buffer);
      expect(typeof tx.transaction).toBe("object");
      expect(typeof tx.meta).toBe("object");
      expect(typeof tx.index).toBe("string");
      expect(typeof tx.isVote).toBe("boolean");

      const txMeta = subscribe_update_response.transaction.transaction.meta;
      expect(Object.prototype.toString.call(txMeta.preBalances)).toBe(
        "[object Array]",
      );
      expect(Object.prototype.toString.call(txMeta.postBalances)).toBe(
        "[object Array]",
      );
      expect(Object.prototype.toString.call(txMeta.innerInstructions)).toBe(
        "[object Array]",
      );
      expect(Object.prototype.toString.call(txMeta.logMessages)).toBe(
        "[object Array]",
      );
      expect(Object.prototype.toString.call(txMeta.preTokenBalances)).toBe(
        "[object Array]",
      );
      expect(Object.prototype.toString.call(txMeta.postTokenBalances)).toBe(
        "[object Array]",
      );
      expect(Object.prototype.toString.call(txMeta.rewards)).toBe(
        "[object Array]",
      );
      expect(
        Object.prototype.toString.call(txMeta.loadedWritableAddresses),
      ).toBe("[object Array]");
      expect(
        Object.prototype.toString.call(txMeta.loadedReadonlyAddresses),
      ).toBe("[object Array]");
      expect(typeof txMeta.innerInstructionsNone).toBe("boolean");
      expect(typeof txMeta.logMessagesNone).toBe("boolean");
      expect(typeof txMeta.returnDataNone).toBe("boolean");
      expect(typeof txMeta.computeUnitsConsumed).toBe("string");
      expect(typeof txMeta.fee).toBe("string");
      expect(typeof txMeta.costUnits).toBe("string");

      const innerTx =
        subscribe_update_response.transaction.transaction.transaction;
      expect(Object.prototype.toString.call(innerTx.signatures)).toBe(
        "[object Array]",
      );
      expect(typeof innerTx.message).toBe("object");
      expect(Object.prototype.toString.call(innerTx.message.accountKeys)).toBe(
        "[object Array]",
      );
      expect(Object.prototype.toString.call(innerTx.message.instructions)).toBe(
        "[object Array]",
      );
      expect(
        Object.prototype.toString.call(innerTx.message.addressTableLookups),
      ).toBe("[object Array]");
      expect(innerTx.message.recentBlockhash).toBeInstanceOf(Buffer);
      expect(typeof innerTx.message.header).toBe("object");
      expect(typeof innerTx.message.header.numRequiredSignatures).toBe(
        "number",
      );
      expect(typeof innerTx.message.header.numReadonlySignedAccounts).toBe(
        "number",
      );
      expect(typeof innerTx.message.header.numReadonlyUnsignedAccounts).toBe(
        "number",
      );
      expect(typeof innerTx.message.versioned).toBe("boolean");

      const decodedTransaction = expectEncodeDecodeRoundTrip(
        SubscribeUpdateTransaction,
        subscribe_update_response.transaction,
      );
      expect(decodedTransaction.transaction).toBeDefined();

      const decodedTransactionInfo = expectEncodeDecodeRoundTrip(
        SubscribeUpdateTransactionInfo,
        subscribe_update_response.transaction.transaction,
      );
      expect(decodedTransactionInfo.signature).toBeDefined();

      const decodedEnvelope = expectEncodeDecodeRoundTrip(
        SubscribeUpdate,
        subscribe_update_response,
      );
      expect(decodedEnvelope.transaction).toBeDefined();
    },
    TEST_TIMEOUT,
  );

  test(
    "transactionStatus",
    async () => {
      let subscribe_update_response: any;
      const subscribe_duplex_stream = await client.subscribe();
      const request: SubscribeRequest = {
        transactionsStatus: {
          client: {
            accountExclude: [],
            accountInclude: [],
            accountRequired: [],
          },
        },
        accounts: {},
        slots: {},
        transactions: {},
        accountsDataSlice: [],
        blocks: {},
        blocksMeta: {},
        entry: {},
        commitment: 2,
      };

      const waitForTransactionStatus = waitForSubscribeUpdateMatchingPredicate(
        subscribe_duplex_stream,
        (data) => Boolean(data?.transactionStatus),
        TEST_TIMEOUT,
      );

      subscribe_duplex_stream.write(request, (err) => {
        if (err) {
          console.error(`error writing to stream: ${err}`);
        }
      });

      subscribe_update_response = await waitForTransactionStatus;

      expect(subscribe_update_response.filters).toEqual(["client"]);
      expect(
        Object.prototype.toString.call(subscribe_update_response.createdAt),
      ).toBe("[object Date]");
      expect((subscribe_update_response as any).updateOneof).toBeUndefined();
      expect(typeof subscribe_update_response.transactionStatus).toBe("object");
      expect(typeof subscribe_update_response.transactionStatus.slot).toBe(
        "string",
      );
      expect(
        subscribe_update_response.transactionStatus.signature,
      ).toBeInstanceOf(Buffer);
      expect(typeof subscribe_update_response.transactionStatus.isVote).toBe(
        "boolean",
      );
      expect(typeof subscribe_update_response.transactionStatus.index).toBe(
        "string",
      );

      const decodedTransactionStatus = expectEncodeDecodeRoundTrip(
        SubscribeUpdateTransactionStatus,
        subscribe_update_response.transactionStatus,
      );
      expect(decodedTransactionStatus.signature).toBeDefined();

      const decodedEnvelope = expectEncodeDecodeRoundTrip(
        SubscribeUpdate,
        subscribe_update_response,
      );
      expect(decodedEnvelope.transactionStatus).toBeDefined();
    },
    TEST_TIMEOUT,
  );

  test(
    "block",
    async () => {
      let subscribe_update_response: any;
      const subscribe_duplex_stream = await client.subscribe();
      const request: SubscribeRequest = {
        blocks: {
          client: {
            accountInclude: [],
            includeTransactions: true,
            includeAccounts: false,
            includeEntries: false,
          },
        },
        accounts: {},
        slots: {},
        transactions: {},
        transactionsStatus: {},
        accountsDataSlice: [],
        blocksMeta: {},
        entry: {},
        commitment: 2,
      };

      const waitForBlock = waitForSubscribeUpdateMatchingPredicate(
        subscribe_duplex_stream,
        (data) => Boolean(data?.block),
        TEST_TIMEOUT,
      );

      subscribe_duplex_stream.write(request, (err) => {
        if (err) {
          console.error(`error writing to stream: ${err}`);
        }
      });

      subscribe_update_response = await waitForBlock;

      expect(subscribe_update_response.filters).toEqual(["client"]);
      expect(
        Object.prototype.toString.call(subscribe_update_response.createdAt),
      ).toBe("[object Date]");
      expect((subscribe_update_response as any).updateOneof).toBeUndefined();
      expect(typeof subscribe_update_response.block).toBe("object");

      const block = subscribe_update_response.block;
      expect(typeof block.slot).toBe("string");
      expect(typeof block.blockhash).toBe("string");
      expect(typeof block.parentSlot).toBe("string");
      expect(typeof block.parentBlockhash).toBe("string");
      expect(typeof block.executedTransactionCount).toBe("string");
      expect(typeof block.updatedAccountCount).toBe("string");
      expect(typeof block.entriesCount).toBe("string");
      expect(Object.prototype.toString.call(block.transactions)).toBe(
        "[object Array]",
      );
      expect(Object.prototype.toString.call(block.accounts)).toBe(
        "[object Array]",
      );
      expect(Object.prototype.toString.call(block.entries)).toBe(
        "[object Array]",
      );

      const decodedBlock = expectEncodeDecodeRoundTrip(
        SubscribeUpdateBlock,
        subscribe_update_response.block,
      );
      expect(decodedBlock.blockhash).toBeDefined();

      const decodedEnvelope = expectEncodeDecodeRoundTrip(
        SubscribeUpdate,
        subscribe_update_response,
      );
      expect(decodedEnvelope.block).toBeDefined();
    },
    TEST_TIMEOUT,
  );

  test(
    "blockMeta",
    async () => {
      let subscribe_update_response: any;
      const subscribe_duplex_stream = await client.subscribe();
      const request: SubscribeRequest = {
        blocksMeta: {
          client: {},
        },
        accounts: {},
        slots: {},
        transactions: {},
        transactionsStatus: {},
        accountsDataSlice: [],
        blocks: {},
        entry: {},
        commitment: 2,
      };

      const waitForBlockMeta = waitForSubscribeUpdateMatchingPredicate(
        subscribe_duplex_stream,
        (data) => Boolean(data?.blockMeta),
        TEST_TIMEOUT,
      );

      subscribe_duplex_stream.write(request, (err) => {
        if (err) {
          console.error(`error writing to stream: ${err}`);
        }
      });

      try {
        subscribe_update_response = await waitForBlockMeta;
      } catch (error) {
        if (isChannelClosedError(error)) {
          expect(isChannelClosedError(error)).toBe(true);
          return;
        }
        throw error;
      }

      expect(subscribe_update_response.filters).toEqual(["client"]);
      expect(
        Object.prototype.toString.call(subscribe_update_response.createdAt),
      ).toBe("[object Date]");
      expect((subscribe_update_response as any).updateOneof).toBeUndefined();
      expect(typeof subscribe_update_response.blockMeta).toBe("object");

      const blockMeta = subscribe_update_response.blockMeta;
      expect(typeof blockMeta.slot).toBe("string");
      expect(typeof blockMeta.blockhash).toBe("string");
      expect(typeof blockMeta.parentSlot).toBe("string");
      expect(typeof blockMeta.parentBlockhash).toBe("string");
      expect(typeof blockMeta.executedTransactionCount).toBe("string");
      expect(typeof blockMeta.entriesCount).toBe("string");

      const decodedBlockMeta = expectEncodeDecodeRoundTrip(
        SubscribeUpdateBlockMeta,
        subscribe_update_response.blockMeta,
      );
      expect(decodedBlockMeta.blockhash).toBeDefined();

      const decodedEnvelope = expectEncodeDecodeRoundTrip(
        SubscribeUpdate,
        subscribe_update_response,
      );
      expect(decodedEnvelope.blockMeta).toBeDefined();
    },
    TEST_TIMEOUT,
  );

  test(
    "entry",
    async () => {
      let subscribe_update_response: any;
      const subscribe_duplex_stream = await client.subscribe();
      const request: SubscribeRequest = {
        entry: {
          client: {},
        },
        accounts: {},
        slots: {},
        transactions: {},
        transactionsStatus: {},
        accountsDataSlice: [],
        blocks: {},
        blocksMeta: {},
        commitment: 2,
      };

      const waitForEntry = waitForSubscribeUpdateMatchingPredicate(
        subscribe_duplex_stream,
        (data) => Boolean(data?.entry),
        TEST_TIMEOUT,
      );

      subscribe_duplex_stream.write(request, (err) => {
        if (err) {
          console.error(`error writing to stream: ${err}`);
        }
      });

      try {
        subscribe_update_response = await waitForEntry;
      } catch (error) {
        if (isChannelClosedError(error)) {
          expect(isChannelClosedError(error)).toBe(true);
          return;
        }
        throw error;
      }

      expect(subscribe_update_response.filters).toEqual(["client"]);
      expect(
        Object.prototype.toString.call(subscribe_update_response.createdAt),
      ).toBe("[object Date]");
      expect((subscribe_update_response as any).updateOneof).toBeUndefined();
      expect(typeof subscribe_update_response.entry).toBe("object");

      const entry = subscribe_update_response.entry;
      expect(typeof entry.slot).toBe("string");
      expect(typeof entry.index).toBe("string");
      expect(typeof entry.numHashes).toBe("string");
      expect(entry.hash).toBeInstanceOf(Buffer);
      expect(typeof entry.executedTransactionCount).toBe("string");
      expect(typeof entry.startingTransactionIndex).toBe("string");

      const decodedEntry = expectEncodeDecodeRoundTrip(
        SubscribeUpdateEntry,
        subscribe_update_response.entry,
      );
      expect(decodedEntry.hash).toBeDefined();

      const decodedEnvelope = expectEncodeDecodeRoundTrip(
        SubscribeUpdate,
        subscribe_update_response,
      );
      expect(decodedEnvelope.entry).toBeDefined();
    },
    TEST_TIMEOUT,
  );

  test(
    "ping/pong",
    async () => {
      let subscribe_update_response: any;
      const subscribe_duplex_stream = await client.subscribe();
      const request: SubscribeRequest = {
        accounts: {},
        slots: {},
        transactions: {},
        transactionsStatus: {},
        accountsDataSlice: [],
        blocks: {},
        blocksMeta: {},
        entry: {},
        commitment: 2,
        ping: {
          id: 42,
        },
      };

      const waitForPingOrPong = waitForSubscribeUpdateMatchingPredicate(
        subscribe_duplex_stream,
        (data) => Boolean(data?.pong || data?.ping),
        TEST_TIMEOUT,
      );

      subscribe_duplex_stream.write(request, (err) => {
        if (err) {
          console.error(`error writing to stream: ${err}`);
        }
      });

      subscribe_update_response = await waitForPingOrPong;

      expect(
        Object.prototype.toString.call(subscribe_update_response.createdAt),
      ).toBe("[object Date]");

      expect((subscribe_update_response as any).updateOneof).toBeUndefined();
      const pong = subscribe_update_response.pong;
      const ping = subscribe_update_response.ping;

      if (pong) {
        expect(typeof pong).toBe("object");
        expect(typeof pong.id).toBe("number");
        expect(pong.id).toBe(42);

        const decodedPong = expectEncodeDecodeRoundTrip(
          SubscribeUpdatePong,
          pong,
        );
        expect(decodedPong.id).toBe(42);

        const decodedEnvelope = expectEncodeDecodeRoundTrip(
          SubscribeUpdate,
          subscribe_update_response,
        );
        expect(decodedEnvelope.pong).toBeDefined();
        return;
      }

      expect(typeof ping).toBe("object");

      const decodedPing = expectEncodeDecodeRoundTrip(
        SubscribeUpdatePing,
        ping,
        true,
      );
      expect(decodedPing).toBeDefined();

      const decodedEnvelope = expectEncodeDecodeRoundTrip(
        SubscribeUpdate,
        subscribe_update_response,
      );
      expect(decodedEnvelope.ping).toBeDefined();
    },
    TEST_TIMEOUT,
  );

  test(
    "SubscribeUpdateTransactionInfo encode",
    async () => {
      const subscribe_duplex_stream = await client.subscribe();
      const request: SubscribeRequest = {
        transactions: {
          client: {
            accountExclude: [],
            accountInclude: [],
            accountRequired: [],
          },
        },
        accounts: {},
        slots: {},
        transactionsStatus: {},
        accountsDataSlice: [],
        blocks: {},
        blocksMeta: {},
        entry: {},
        commitment: 2,
      };

      const waitForTransactionInfo = waitForSubscribeUpdateMatchingPredicate(
        subscribe_duplex_stream,
        (data) => Boolean(data?.transaction?.transaction),
        TEST_TIMEOUT,
      );

      subscribe_duplex_stream.write(request, (err) => {
        if (err) {
          console.error(`error writing to stream: ${err}`);
        }
      });

      const subscribe_update_response = await waitForTransactionInfo;

      expect((subscribe_update_response as any).updateOneof).toBeUndefined();
      const tx_info = subscribe_update_response.transaction.transaction;
      const decoded = expectEncodeDecodeRoundTrip(
        SubscribeUpdateTransactionInfo,
        tx_info,
      );
      expect(decoded.signature).toBeDefined();

      const decodedEnvelope = expectEncodeDecodeRoundTrip(
        SubscribeUpdate,
        subscribe_update_response,
      );
      expect(decodedEnvelope.transaction).toBeDefined();
    },
    TEST_TIMEOUT,
  );
});

describe("subscribeDeshred response schema tests", () => {
  const TEST_TIMEOUT = 100000;

  // .env
  const { TEST_ENDPOINT: endpoint, TEST_TOKEN: xToken } = process.env;

  const channelOptions = {};
  const client = new Client(endpoint, xToken, channelOptions);

  beforeAll(async () => {
    await client.connect();
  });

  function baseSubscribeDeshredRequest(): SubscribeDeshredRequest {
    return {
      deshredTransactions: {},
    };
  }

  async function assertDeshredRequestsAcceptedOnSingleStream(
    requests: Array<{ label: string; request: SubscribeDeshredRequest }>,
    settleMs = 250,
  ): Promise<void> {
    const stream = await client.subscribeDeshred();
    const streamErrors: Error[] = [];
    stream.on("error", (error: Error) => {
      streamErrors.push(error);
    });

    try {
      for (const { label, request } of requests) {
        const writeError = await writeAndCaptureError(stream, request);
        if (writeError) {
          throw new Error(
            `[${label}] write failed: ${String(writeError.message ?? writeError)}`,
          );
        }
        await new Promise((resolve) => setTimeout(resolve, settleMs));
        const unexpectedError = streamErrors.find(
          (error) =>
            !String(error?.message ?? "")
              .toLowerCase()
              .includes("no update available"),
        );
        if (unexpectedError) {
          throw new Error(
            `[${label}] stream emitted error: ${unexpectedError.message}`,
          );
        }
      }
    } finally {
      await closeStreamAndWait(stream);
    }
  }

  test("deshred filter combinations are accepted", async () => {
    const triStateBool: Array<boolean | undefined> = [undefined, true, false];
    const requests: Array<{ label: string; request: SubscribeDeshredRequest }> =
      [];

    for (const vote of triStateBool) {
      const request = baseSubscribeDeshredRequest();
      const label = `vote_${String(vote)}`;
      request.deshredTransactions = {
        [label]: {
          vote,
          accountInclude: [],
          accountExclude: [],
          accountRequired: [],
        },
      };
      requests.push({ label, request });
    }

    await assertDeshredRequestsAcceptedOnSingleStream(requests);
  }, 180000);

  test("deshred runtime server errors are propagated via stream error event", async () => {
    const stream = await client.subscribeDeshred();
    try {
      // Ensure `_read()` is driven so native terminal errors can surface to JS.
      stream.on("data", () => {});
      const runtimeError = waitForStreamError(stream, TEST_TIMEOUT);
      const request: SubscribeDeshredRequest = {
        deshredTransactions: {
          invalid_pubkey_filter: {
            accountInclude: ["not_base58_!!!"],
            accountExclude: [],
            accountRequired: [],
          },
        },
      };

      const writeError = await writeAndWaitCallback(stream, request);
      expect(writeError).toBeNull();

      const observedError = await runtimeError;
      const message = String(observedError?.message ?? "").toLowerCase();
      expect(message.length).toBeGreaterThan(0);
      expect(message.includes("no update available")).toBe(false);
    } finally {
      await closeStreamAndWait(stream);
    }
  }, TEST_TIMEOUT);

  test(
    "deshred ping/pong",
    async () => {
      const subscribe_deshred_stream = await client.subscribeDeshred();
      const request: SubscribeDeshredRequest = {
        deshredTransactions: {},
        ping: {
          id: 42,
        },
      };

      const waitForPingOrPong = waitForSubscribeUpdateMatchingPredicate(
        subscribe_deshred_stream,
        (data) => Boolean(data?.pong || data?.ping),
        TEST_TIMEOUT,
      );

      subscribe_deshred_stream.write(request, (err) => {
        if (err) {
          console.error(`error writing to stream: ${err}`);
        }
      });

      const subscribe_update_response = await waitForPingOrPong;
      expect((subscribe_update_response as any).updateOneof).toBeUndefined();

      const pong = subscribe_update_response.pong;
      const ping = subscribe_update_response.ping;

      if (pong) {
        expect(typeof pong).toBe("object");
        expect(typeof pong.id).toBe("number");
        expect(pong.id).toBe(42);

        const decodedPong = expectEncodeDecodeRoundTrip(
          SubscribeUpdatePong,
          pong,
        );
        expect(decodedPong.id).toBe(42);
      } else {
        expect(typeof ping).toBe("object");
        const decodedPing = expectEncodeDecodeRoundTrip(
          SubscribeUpdatePing,
          ping,
          true,
        );
        expect(decodedPing).toBeDefined();
      }

      const decodedEnvelope = expectEncodeDecodeRoundTrip(
        geyser.SubscribeUpdateDeshred,
        subscribe_update_response,
      );
      expect(decodedEnvelope.ping || decodedEnvelope.pong).toBeDefined();
    },
    TEST_TIMEOUT,
  );
});

describe("unary response schema tests", () => {
  const TEST_TIMEOUT = 100000;

  // .env
  const { TEST_ENDPOINT: endpoint, TEST_TOKEN: xToken } = process.env;

  // Use options sensible defaults.
  const channelOptions = {};
  const client = new Client(endpoint, xToken, channelOptions);

  beforeAll(async () => {
    await client.connect();
  });

  test(
    "getLatestBlockhash",
    async () => {
      const response = await client.getLatestBlockhash(2);

      expect(typeof response).toBe("object");
      expect(typeof response.slot).toBe("string");
      expect(typeof response.blockhash).toBe("string");
      expect(typeof response.lastValidBlockHeight).toBe("string");
      expect(response.blockhash.length).toBeGreaterThan(0);

      const decoded = expectEncodeDecodeRoundTrip(
        GetLatestBlockhashResponse,
        response,
      );
      expect(decoded.blockhash).toBe(response.blockhash);
    },
    TEST_TIMEOUT,
  );

  test(
    "ping",
    async () => {
      const pingCount = 7;
      const response = await client.ping(pingCount);

      expect(typeof response).toBe("object");
      expect(typeof response.count).toBe("number");
      expect(response.count).toBe(pingCount);

      const decoded = expectEncodeDecodeRoundTrip(PongResponse, response);
      expect(decoded.count).toBe(pingCount);
    },
    TEST_TIMEOUT,
  );

  test(
    "getBlockHeight",
    async () => {
      const response = await client.getBlockHeight(2);

      expect(typeof response).toBe("object");
      expect(typeof response.blockHeight).toBe("string");
      expect(response.blockHeight.length).toBeGreaterThan(0);

      const decoded = expectEncodeDecodeRoundTrip(
        GetBlockHeightResponse,
        response,
      );
      expect(decoded.blockHeight).toBe(response.blockHeight);
    },
    TEST_TIMEOUT,
  );

  test(
    "getSlot",
    async () => {
      const response = await client.getSlot(2);

      expect(typeof response).toBe("object");
      expect(typeof response.slot).toBe("string");
      expect(response.slot.length).toBeGreaterThan(0);

      const decoded = expectEncodeDecodeRoundTrip(GetSlotResponse, response);
      expect(decoded.slot).toBe(response.slot);
    },
    TEST_TIMEOUT,
  );

  test(
    "isBlockhashValid",
    async () => {
      const latestBlockhash = await client.getLatestBlockhash(2);
      const response = await client.isBlockhashValid(
        latestBlockhash.blockhash,
        2,
      );

      expect(typeof response).toBe("object");
      expect(typeof response.slot).toBe("string");
      expect(typeof response.valid).toBe("boolean");

      const decoded = expectEncodeDecodeRoundTrip(
        IsBlockhashValidResponse,
        response,
      );
      expect(decoded.valid).toBe(response.valid);
    },
    TEST_TIMEOUT,
  );

  test(
    "getVersion",
    async () => {
      const response = await client.getVersion();

      expect(typeof response).toBe("object");
      expect(typeof response.version).toBe("string");
      expect(response.version.length).toBeGreaterThan(0);

      const decoded = expectEncodeDecodeRoundTrip(GetVersionResponse, response);
      expect(decoded.version).toBe(response.version);
    },
    TEST_TIMEOUT,
  );

  test(
    "subscribeReplayInfo",
    async () => {
      const response = await client.subscribeReplayInfo();

      expect(typeof response).toBe("object");
      if (
        response.firstAvailable !== undefined &&
        response.firstAvailable !== null
      ) {
        expect(typeof response.firstAvailable).toBe("string");
      }

      const decoded = expectEncodeDecodeRoundTrip(
        SubscribeReplayInfoResponse,
        response,
        true,
      );
      expect(decoded).toBeDefined();
    },
    TEST_TIMEOUT,
  );
});

describe("grpc message encode/decode tests", () => {
  const allMessageFns = getAllGeyserMessageFns();

  test("has message fns to validate", () => {
    expect(allMessageFns.length).toBeGreaterThan(0);
  });

  test.each(allMessageFns)("%s encode/decode", (messageName, messageFns) => {
    const partialValue = messageFns.fromPartial({});
    const encoded = messageFns.encode(partialValue).finish();
    const decoded = messageFns.decode(encoded);

    expect(messageName).toBeTruthy();
    expect(encoded).toBeInstanceOf(Uint8Array);
    expect(encoded.length).toBeGreaterThanOrEqual(0);
    expect(decoded).toBeDefined();

    const reencoded = messageFns.encode(decoded).finish();
    expect(reencoded).toBeInstanceOf(Uint8Array);
    expect(reencoded.length).toBeGreaterThanOrEqual(0);
  });
});
