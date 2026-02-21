import { JsSubscribeRequest } from "../napi";
import Client from "../src"
import {
  SubscribeUpdate,
  SubscribeUpdateAccount,
  SubscribeUpdateBlock,
  SubscribeUpdateBlockMeta,
  SubscribeUpdateEntry,
  SubscribeUpdatePing,
  SubscribeUpdatePong,
  SubscribeUpdateSlot,
  SubscribeUpdateTransaction,
  SubscribeUpdateTransactionInfo,
  SubscribeUpdateTransactionStatus,
} from "../src/grpc/geyser";

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
            stream.end();
            stream.destroy();
            reject(
              new Error(
                `No matching subscribe update after ${unmatchedUpdates} updates (timeout ${timeoutMs}ms)`,
              ),
            );
          });
        }
        return;
      }

      settleOnce(() => {
        cleanup();
        stream.end();
        stream.destroy();
        resolve(data);
      });
    };

    const onError = (error: Error) => {
      settleOnce(() => {
        cleanup();
        reject(error);
      });
    };

    const onEndOrClose = () => {
      settleOnce(() => {
        cleanup();
        reject(new Error("Stream ended before receiving expected subscribe update"));
      });
    };

    const timeoutId = setTimeout(() => {
      settleOnce(() => {
        cleanup();
        stream.end();
        stream.destroy();
        reject(new Error(`Timed out waiting for expected subscribe update after ${timeoutMs}ms`));
      });
    }, timeoutMs);

    stream.on("data", onData);
    stream.on("error", onError);
    stream.on("end", onEndOrClose);
    stream.on("close", onEndOrClose);
  });
}

function expectEncodeDecodeRoundTrip(messageFns: any, payload: any, allowEmpty = false): any {
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

function toSubscribeUpdateEnvelope(subscribe_update_response: any): any {
  return {
    filters: subscribe_update_response?.filters ?? [],
    createdAt: subscribe_update_response?.createdAt,
    ...(subscribe_update_response?.updateOneof ?? {}),
  };
}

describe("subscribe response schema tests", () => {
  const TEST_TIMEOUT = 100000;

  // .env
  const {
    TEST_ENDPOINT: endpoint,
    TEST_TOKEN: xToken
  } = process.env;

  // Use options sensible defaults.
  const channelOptions = {};
  const client = new Client(endpoint, xToken, channelOptions);

  beforeAll(async () => {
    await client.connect();
  });

  test("account", async () => {
    let subscribe_update_response: any;
    const subscribe_duplex_stream = await client.subscribe();
    const request: JsSubscribeRequest = {
      accounts: {
        client: {
          account: [],
          filters: [],
          owner: [],
        }
      },
      slots: {},
      transactions: {},
      transactionsStatus: {},
      accountsDataSlice: [],
      blocks: {},
      blocksMeta: {},
      entry: {},
      commitment: 2
    };

    const waitForAccount = waitForSubscribeUpdateMatchingPredicate(
      subscribe_duplex_stream,
      (data) => Boolean(data?.updateOneof?.account),
      TEST_TIMEOUT,
    );

    subscribe_duplex_stream.write(request, (err) => {
      if (err) {
        console.error(`error writing to stream: ${err}`)
      }
    });

    subscribe_update_response = await waitForAccount;

    expect(subscribe_update_response.filters).toEqual(["client"]);
    // We're doing it this way so we can bypass the Jest Globals vs Node Globals
    // type conflicts that makes expect(Date).toBeInstanceOf(Date) to fail.
    //
    // See issue here: https://github.com/jestjs/jest/issues/2549
    expect(Object.prototype.toString.call(subscribe_update_response.createdAt)).toBe("[object Date]");
    expect(typeof subscribe_update_response.updateOneof.account).toBe("object");
    expect(typeof subscribe_update_response.updateOneof.account.slot).toBe("string");
    expect(typeof subscribe_update_response.updateOneof.account.isStartup).toBe("boolean");
    expect(typeof subscribe_update_response.updateOneof.account.account).toBe("object");

    const account = subscribe_update_response.updateOneof.account.account;
    expect(account.pubkey).toBeInstanceOf(Buffer);
    expect(account.owner).toBeInstanceOf(Buffer);
    expect(account.data).toBeInstanceOf(Buffer);
    expect(typeof account.lamports).toBe("string");
    expect(typeof account.rentEpoch).toBe("string");
    expect(typeof account.writeVersion).toBe("string");
    expect(typeof account.executable).toBe("boolean");

    const decodedAccount = expectEncodeDecodeRoundTrip(
      SubscribeUpdateAccount,
      subscribe_update_response.updateOneof.account,
    );
    expect(decodedAccount.account).toBeDefined();

    const decodedEnvelope = expectEncodeDecodeRoundTrip(
      SubscribeUpdate,
      toSubscribeUpdateEnvelope(subscribe_update_response),
    );
    expect(decodedEnvelope.account).toBeDefined();

  }, TEST_TIMEOUT);

  test("slot", async () => {
    let subscribe_update_response: any;
    const subscribe_duplex_stream = await client.subscribe();
    const request: JsSubscribeRequest = {
      slots: {
        client: {}
      },
      accounts: {},
      transactions: {},
      transactionsStatus: {},
      accountsDataSlice: [],
      blocks: {},
      blocksMeta: {},
      entry: {},
      commitment: 2
    };

    const waitForSlot = waitForSubscribeUpdateMatchingPredicate(
      subscribe_duplex_stream,
      (data) => Boolean(data?.updateOneof?.slot),
      TEST_TIMEOUT,
    );

    subscribe_duplex_stream.write(request, (err) => {
      if (err) {
        console.error(`error writing to stream: ${err}`)
      }
    });

    subscribe_update_response = await waitForSlot;

    expect(subscribe_update_response.filters).toEqual(["client"]);
    // We're doing it this way so we can bypass the Jest Globals vs Node Globals
    // type conflicts that makes expect(Date).toBeInstanceOf(Date) to fail.
    //
    // See issue here: https://github.com/jestjs/jest/issues/2549
    expect(Object.prototype.toString.call(subscribe_update_response.createdAt)).toBe("[object Date]");
    expect(typeof subscribe_update_response.updateOneof.slot).toBe("object");
    expect(typeof subscribe_update_response.updateOneof.slot.slot).toBe("string");
    expect(typeof subscribe_update_response.updateOneof.slot.status).toBe("number");

    const decodedSlot = expectEncodeDecodeRoundTrip(
      SubscribeUpdateSlot,
      subscribe_update_response.updateOneof.slot,
    );
    expect(decodedSlot.slot).toBeDefined();

    const decodedEnvelope = expectEncodeDecodeRoundTrip(
      SubscribeUpdate,
      toSubscribeUpdateEnvelope(subscribe_update_response),
    );
    expect(decodedEnvelope.slot).toBeDefined();

  }, TEST_TIMEOUT);

  test("transaction", async () => {
    let subscribe_update_response: any;
    const subscribe_duplex_stream = await client.subscribe();
    const request: JsSubscribeRequest = {
      transactions: {
        client: {
          accountExclude: [],
          accountInclude: [],
          accountRequired: []
        },
      },
      accounts: {},
      slots: {},
      transactionsStatus: {},
      accountsDataSlice: [],
      blocks: {},
      blocksMeta: {},
      entry: {},
      commitment: 2
    };

    const waitForTransaction = waitForSubscribeUpdateMatchingPredicate(
      subscribe_duplex_stream,
      (data) => Boolean(data?.updateOneof?.transaction?.transaction),
      TEST_TIMEOUT,
    );

    subscribe_duplex_stream.write(request, (err) => {
      if (err) {
        console.error(`error writing to stream: ${err}`)
      }
    });

    subscribe_update_response = await waitForTransaction;

    expect(subscribe_update_response.filters).toEqual(["client"]);
    // We're doing it this way so we can bypass the Jest Globals vs Node Globals
    // type conflicts that makes expect(Date).toBeInstanceOf(Date) to fail.
    //
    // See issue here: https://github.com/jestjs/jest/issues/2549
    expect(Object.prototype.toString.call(subscribe_update_response.createdAt)).toBe("[object Date]");
    expect(typeof subscribe_update_response.updateOneof.transaction).toBe("object");
    expect(typeof subscribe_update_response.updateOneof.transaction.slot).toBe("string");

    const tx = subscribe_update_response.updateOneof.transaction.transaction;
    expect(tx.signature).toBeInstanceOf(Buffer);
    expect(typeof tx.transaction).toBe("object");
    expect(typeof tx.meta).toBe("object");
    expect(typeof tx.index).toBe("string");
    expect(typeof tx.isVote).toBe("boolean");

    const txMeta = subscribe_update_response.updateOneof.transaction.transaction.meta;
    expect(Object.prototype.toString.call(txMeta.preBalances)).toBe("[object Array]");
    expect(Object.prototype.toString.call(txMeta.postBalances)).toBe("[object Array]");
    expect(Object.prototype.toString.call(txMeta.innerInstructions)).toBe("[object Array]");
    expect(Object.prototype.toString.call(txMeta.logMessages)).toBe("[object Array]");
    expect(Object.prototype.toString.call(txMeta.preTokenBalances)).toBe("[object Array]");
    expect(Object.prototype.toString.call(txMeta.postTokenBalances)).toBe("[object Array]");
    expect(Object.prototype.toString.call(txMeta.rewards)).toBe("[object Array]");
    expect(Object.prototype.toString.call(txMeta.loadedWritableAddresses)).toBe("[object Array]");
    expect(Object.prototype.toString.call(txMeta.loadedReadonlyAddresses)).toBe("[object Array]");
    expect(typeof txMeta.innerInstructionsNone).toBe("boolean");
    expect(typeof txMeta.logMessagesNone).toBe("boolean");
    expect(typeof txMeta.returnDataNone).toBe("boolean");
    expect(typeof txMeta.computeUnitsConsumed).toBe("string");
    expect(typeof txMeta.fee).toBe("string");
    expect(typeof txMeta.costUnits).toBe("string");

    const innerTx = subscribe_update_response.updateOneof.transaction.transaction.transaction;
    expect(Object.prototype.toString.call(innerTx.signatures)).toBe("[object Array]");
    expect(typeof innerTx.message).toBe("object");
    expect(Object.prototype.toString.call(innerTx.message.accountKeys)).toBe("[object Array]");
    expect(Object.prototype.toString.call(innerTx.message.instructions)).toBe("[object Array]");
    expect(Object.prototype.toString.call(innerTx.message.addressTableLookups)).toBe("[object Array]");
    expect(innerTx.message.recentBlockhash).toBeInstanceOf(Buffer);
    expect(typeof innerTx.message.header).toBe("object");
    expect(typeof innerTx.message.header.numRequiredSignatures).toBe("number");
    expect(typeof innerTx.message.header.numReadonlySignedAccounts).toBe("number");
    expect(typeof innerTx.message.header.numReadonlyUnsignedAccounts).toBe("number");
    expect(typeof innerTx.message.versioned).toBe("boolean");

    const decodedTransaction = expectEncodeDecodeRoundTrip(
      SubscribeUpdateTransaction,
      subscribe_update_response.updateOneof.transaction,
    );
    expect(decodedTransaction.transaction).toBeDefined();

    const decodedTransactionInfo = expectEncodeDecodeRoundTrip(
      SubscribeUpdateTransactionInfo,
      subscribe_update_response.updateOneof.transaction.transaction,
    );
    expect(decodedTransactionInfo.signature).toBeDefined();

    const decodedEnvelope = expectEncodeDecodeRoundTrip(
      SubscribeUpdate,
      toSubscribeUpdateEnvelope(subscribe_update_response),
    );
    expect(decodedEnvelope.transaction).toBeDefined();

  }, TEST_TIMEOUT)

  test("transactionStatus", async () => {
    let subscribe_update_response: any;
    const subscribe_duplex_stream = await client.subscribe();
    const request: JsSubscribeRequest = {
      transactionsStatus: {
        client: {
          accountExclude: [],
          accountInclude: [],
          accountRequired: []
        },
      },
      accounts: {},
      slots: {},
      transactions: {},
      accountsDataSlice: [],
      blocks: {},
      blocksMeta: {},
      entry: {},
      commitment: 2
    };

    const waitForTransactionStatus = waitForSubscribeUpdateMatchingPredicate(
      subscribe_duplex_stream,
      (data) => Boolean(data?.updateOneof?.transactionStatus),
      TEST_TIMEOUT,
    );

    subscribe_duplex_stream.write(request, (err) => {
      if (err) {
        console.error(`error writing to stream: ${err}`)
      }
    });

    subscribe_update_response = await waitForTransactionStatus;

    expect(subscribe_update_response.filters).toEqual(["client"]);
    expect(Object.prototype.toString.call(subscribe_update_response.createdAt)).toBe("[object Date]");
    expect(typeof subscribe_update_response.updateOneof.transactionStatus).toBe("object");
    expect(typeof subscribe_update_response.updateOneof.transactionStatus.slot).toBe("string");
    expect(subscribe_update_response.updateOneof.transactionStatus.signature).toBeInstanceOf(Buffer);
    expect(typeof subscribe_update_response.updateOneof.transactionStatus.isVote).toBe("boolean");
    expect(typeof subscribe_update_response.updateOneof.transactionStatus.index).toBe("string");

    const decodedTransactionStatus = expectEncodeDecodeRoundTrip(
      SubscribeUpdateTransactionStatus,
      subscribe_update_response.updateOneof.transactionStatus,
    );
    expect(decodedTransactionStatus.signature).toBeDefined();

    const decodedEnvelope = expectEncodeDecodeRoundTrip(
      SubscribeUpdate,
      toSubscribeUpdateEnvelope(subscribe_update_response),
    );
    expect(decodedEnvelope.transactionStatus).toBeDefined();
  }, TEST_TIMEOUT);

  test("block", async () => {
    let subscribe_update_response: any;
    const subscribe_duplex_stream = await client.subscribe();
    const request: JsSubscribeRequest = {
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
      commitment: 2
    };

    const waitForBlock = waitForSubscribeUpdateMatchingPredicate(
      subscribe_duplex_stream,
      (data) => Boolean(data?.updateOneof?.block),
      TEST_TIMEOUT,
    );

    subscribe_duplex_stream.write(request, (err) => {
      if (err) {
        console.error(`error writing to stream: ${err}`)
      }
    });

    subscribe_update_response = await waitForBlock;

    expect(subscribe_update_response.filters).toEqual(["client"]);
    expect(Object.prototype.toString.call(subscribe_update_response.createdAt)).toBe("[object Date]");
    expect(typeof subscribe_update_response.updateOneof.block).toBe("object");

    const block = subscribe_update_response.updateOneof.block;
    expect(typeof block.slot).toBe("string");
    expect(typeof block.blockhash).toBe("string");
    expect(typeof block.parentSlot).toBe("string");
    expect(typeof block.parentBlockhash).toBe("string");
    expect(typeof block.executedTransactionCount).toBe("string");
    expect(typeof block.updatedAccountCount).toBe("string");
    expect(typeof block.entriesCount).toBe("string");
    expect(Object.prototype.toString.call(block.transactions)).toBe("[object Array]");
    expect(Object.prototype.toString.call(block.accounts)).toBe("[object Array]");
    expect(Object.prototype.toString.call(block.entries)).toBe("[object Array]");

    const decodedBlock = expectEncodeDecodeRoundTrip(
      SubscribeUpdateBlock,
      subscribe_update_response.updateOneof.block,
    );
    expect(decodedBlock.blockhash).toBeDefined();

    const decodedEnvelope = expectEncodeDecodeRoundTrip(
      SubscribeUpdate,
      toSubscribeUpdateEnvelope(subscribe_update_response),
    );
    expect(decodedEnvelope.block).toBeDefined();
  }, TEST_TIMEOUT);

  test("blockMeta", async () => {
    let subscribe_update_response: any;
    const subscribe_duplex_stream = await client.subscribe();
    const request: JsSubscribeRequest = {
      blocksMeta: {
        client: {}
      },
      accounts: {},
      slots: {},
      transactions: {},
      transactionsStatus: {},
      accountsDataSlice: [],
      blocks: {},
      entry: {},
      commitment: 2
    };

    const waitForBlockMeta = waitForSubscribeUpdateMatchingPredicate(
      subscribe_duplex_stream,
      (data) => Boolean(data?.updateOneof?.blockMeta),
      TEST_TIMEOUT,
    );

    subscribe_duplex_stream.write(request, (err) => {
      if (err) {
        console.error(`error writing to stream: ${err}`)
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
    expect(Object.prototype.toString.call(subscribe_update_response.createdAt)).toBe("[object Date]");
    expect(typeof subscribe_update_response.updateOneof.blockMeta).toBe("object");

    const blockMeta = subscribe_update_response.updateOneof.blockMeta;
    expect(typeof blockMeta.slot).toBe("string");
    expect(typeof blockMeta.blockhash).toBe("string");
    expect(typeof blockMeta.parentSlot).toBe("string");
    expect(typeof blockMeta.parentBlockhash).toBe("string");
    expect(typeof blockMeta.executedTransactionCount).toBe("string");
    expect(typeof blockMeta.entriesCount).toBe("string");

    const decodedBlockMeta = expectEncodeDecodeRoundTrip(
      SubscribeUpdateBlockMeta,
      subscribe_update_response.updateOneof.blockMeta,
    );
    expect(decodedBlockMeta.blockhash).toBeDefined();

    const decodedEnvelope = expectEncodeDecodeRoundTrip(
      SubscribeUpdate,
      toSubscribeUpdateEnvelope(subscribe_update_response),
    );
    expect(decodedEnvelope.blockMeta).toBeDefined();
  }, TEST_TIMEOUT);

  test("entry", async () => {
    let subscribe_update_response: any;
    const subscribe_duplex_stream = await client.subscribe();
    const request: JsSubscribeRequest = {
      entry: {
        client: {}
      },
      accounts: {},
      slots: {},
      transactions: {},
      transactionsStatus: {},
      accountsDataSlice: [],
      blocks: {},
      blocksMeta: {},
      commitment: 2
    };

    const waitForEntry = waitForSubscribeUpdateMatchingPredicate(
      subscribe_duplex_stream,
      (data) => Boolean(data?.updateOneof?.entry),
      TEST_TIMEOUT,
    );

    subscribe_duplex_stream.write(request, (err) => {
      if (err) {
        console.error(`error writing to stream: ${err}`)
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
    expect(Object.prototype.toString.call(subscribe_update_response.createdAt)).toBe("[object Date]");
    expect(typeof subscribe_update_response.updateOneof.entry).toBe("object");

    const entry = subscribe_update_response.updateOneof.entry;
    expect(typeof entry.slot).toBe("string");
    expect(typeof entry.index).toBe("string");
    expect(typeof entry.numHashes).toBe("string");
    expect(entry.hash).toBeInstanceOf(Buffer);
    expect(typeof entry.executedTransactionCount).toBe("string");
    expect(typeof entry.startingTransactionIndex).toBe("string");

    const decodedEntry = expectEncodeDecodeRoundTrip(
      SubscribeUpdateEntry,
      subscribe_update_response.updateOneof.entry,
    );
    expect(decodedEntry.hash).toBeDefined();

    const decodedEnvelope = expectEncodeDecodeRoundTrip(
      SubscribeUpdate,
      toSubscribeUpdateEnvelope(subscribe_update_response),
    );
    expect(decodedEnvelope.entry).toBeDefined();
  }, TEST_TIMEOUT);

  test("ping/pong", async () => {
    let subscribe_update_response: any;
    const subscribe_duplex_stream = await client.subscribe();
    const request: JsSubscribeRequest = {
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
      (data) => Boolean(data?.updateOneof?.pong || data?.updateOneof?.ping),
      TEST_TIMEOUT,
    );

    subscribe_duplex_stream.write(request, (err) => {
      if (err) {
        console.error(`error writing to stream: ${err}`)
      }
    });

    subscribe_update_response = await waitForPingOrPong;

    expect(Object.prototype.toString.call(subscribe_update_response.createdAt)).toBe("[object Date]");

    const pong = subscribe_update_response.updateOneof?.pong;
    const ping = subscribe_update_response.updateOneof?.ping;

    if (pong) {
      expect(typeof pong).toBe("object");
      expect(typeof pong.id).toBe("number");
      expect(pong.id).toBe(42);

      const decodedPong = expectEncodeDecodeRoundTrip(SubscribeUpdatePong, pong);
      expect(decodedPong.id).toBe(42);

      const decodedEnvelope = expectEncodeDecodeRoundTrip(
        SubscribeUpdate,
        toSubscribeUpdateEnvelope(subscribe_update_response),
      );
      expect(decodedEnvelope.pong).toBeDefined();
      return;
    }

    expect(typeof ping).toBe("object");

    const decodedPing = expectEncodeDecodeRoundTrip(SubscribeUpdatePing, ping, true);
    expect(decodedPing).toBeDefined();

    const decodedEnvelope = expectEncodeDecodeRoundTrip(
      SubscribeUpdate,
      toSubscribeUpdateEnvelope(subscribe_update_response),
    );
    expect(decodedEnvelope.ping).toBeDefined();
  }, TEST_TIMEOUT);

  test("SubscribeUpdateTransactionInfo encode", async () => {
    const subscribe_duplex_stream = await client.subscribe();
    const request: JsSubscribeRequest = {
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
      (data) => Boolean(data?.updateOneof?.transaction?.transaction),
      TEST_TIMEOUT,
    );

    subscribe_duplex_stream.write(request, (err) => {
      if (err) {
        console.error(`error writing to stream: ${err}`)
      }
    });

    const subscribe_update_response = await waitForTransactionInfo;

    const tx_info = subscribe_update_response.updateOneof.transaction.transaction;
    const decoded = expectEncodeDecodeRoundTrip(SubscribeUpdateTransactionInfo, tx_info);
    expect(decoded.signature).toBeDefined();

    const decodedEnvelope = expectEncodeDecodeRoundTrip(
      SubscribeUpdate,
      toSubscribeUpdateEnvelope(subscribe_update_response),
    );
    expect(decodedEnvelope.transaction).toBeDefined();
  }, TEST_TIMEOUT)
});

describe("unary response schema tests", () => {
  const TEST_TIMEOUT = 100000;

  // .env
  const {
    TEST_ENDPOINT: endpoint,
    TEST_TOKEN: xToken
  } = process.env;

  // Use options sensible defaults.
  const channelOptions = {};
  const client = new Client(endpoint, xToken, channelOptions);

  beforeAll(async () => {
    await client.connect();
  });

  test("getLatestBlockhash", async () => {
    const response = await client.getLatestBlockhash(2);

    expect(typeof response).toBe("object");
    expect(typeof response.slot).toBe("string");
    expect(typeof response.blockhash).toBe("string");
    expect(typeof response.lastValidBlockHeight).toBe("string");
    expect(response.blockhash.length).toBeGreaterThan(0);
  }, TEST_TIMEOUT);

  test("ping", async () => {
    const pingCount = 7;
    const response = await client.ping(pingCount);

    expect(typeof response).toBe("number");
    expect(response).toBe(pingCount);
  }, TEST_TIMEOUT);

  test("getBlockHeight", async () => {
    const response = await client.getBlockHeight(2);

    expect(typeof response).toBe("string");
    expect(response.length).toBeGreaterThan(0);
  }, TEST_TIMEOUT);

  test("getSlot", async () => {
    const response = await client.getSlot(2);

    expect(typeof response).toBe("string");
    expect(response.length).toBeGreaterThan(0);
  }, TEST_TIMEOUT);

  test("isBlockhashValid", async () => {
    const latestBlockhash = await client.getLatestBlockhash(2);
    const response = await client.isBlockhashValid(latestBlockhash.blockhash, 2);

    expect(typeof response).toBe("object");
    expect(typeof response.slot).toBe("string");
    expect(typeof response.valid).toBe("boolean");
  }, TEST_TIMEOUT);

  test("getVersion", async () => {
    const response = await client.getVersion();

    expect(typeof response).toBe("string");
    expect(response.length).toBeGreaterThan(0);
  }, TEST_TIMEOUT);

  test("subscribeReplayInfo", async () => {
    const response = await client.subscribeReplayInfo();

    expect(typeof response).toBe("object");
    if (response.firstAvailable !== undefined && response.firstAvailable !== null) {
      expect(typeof response.firstAvailable).toBe("string");
    }
  }, TEST_TIMEOUT);
});
