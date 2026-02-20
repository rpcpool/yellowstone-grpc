import { JsSubscribeRequest } from "../napi";
import Client, { SubscribeUpdateTransactionInfo } from "../src"

function waitForSubscribeUpdateMatchingPredicate(
  stream: any,
  predicate: (data: any) => boolean,
  timeoutMs: number,
): Promise<any> {
  return new Promise((resolve, reject) => {
    const cleanup = () => {
      clearTimeout(timeoutId);
      stream.off("data", onData);
      stream.off("error", onError);
      stream.off("end", onEndOrClose);
      stream.off("close", onEndOrClose);
    };

    const onData = (data: any) => {
      if (!predicate(data)) {
        return;
      }

      cleanup();
      stream.end();
      stream.destroy();
      resolve(data);
    };

    const onError = (error: Error) => {
      cleanup();
      reject(error);
    };

    const onEndOrClose = () => {
      cleanup();
      reject(new Error("Stream ended before receiving expected subscribe update"));
    };

    const timeoutId = setTimeout(() => {
      cleanup();
      stream.end();
      stream.destroy();
      reject(new Error(`Timed out waiting for expected subscribe update after ${timeoutMs}ms`));
    }, timeoutMs);

    stream.on("data", onData);
    stream.on("error", onError);
    stream.on("end", onEndOrClose);
    stream.on("close", onEndOrClose);
  });
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

    subscribe_duplex_stream.write(request, (err) => {
      if (err) {
        console.error(`error writing to stream: ${err}`)
      }
    });

    subscribe_update_response = await waitForSubscribeUpdateMatchingPredicate(
      subscribe_duplex_stream,
      (data) => Boolean(data?.updateOneof?.account),
      TEST_TIMEOUT,
    );

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

    subscribe_duplex_stream.write(request, (err) => {
      if (err) {
        console.error(`error writing to stream: ${err}`)
      }
    });

    subscribe_update_response = await waitForSubscribeUpdateMatchingPredicate(
      subscribe_duplex_stream,
      (data) => Boolean(data?.updateOneof?.slot),
      TEST_TIMEOUT,
    );

    expect(subscribe_update_response.filters).toEqual(["client"]);
    // We're doing it this way so we can bypass the Jest Globals vs Node Globals
    // type conflicts that makes expect(Date).toBeInstanceOf(Date) to fail.
    //
    // See issue here: https://github.com/jestjs/jest/issues/2549
    expect(Object.prototype.toString.call(subscribe_update_response.createdAt)).toBe("[object Date]");
    expect(typeof subscribe_update_response.updateOneof.slot).toBe("object");
    expect(typeof subscribe_update_response.updateOneof.slot.slot).toBe("string");
    expect(typeof subscribe_update_response.updateOneof.slot.status).toBe("number");

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

    subscribe_duplex_stream.write(request, (err) => {
      if (err) {
        console.error(`error writing to stream: ${err}`)
      }
    });

    subscribe_update_response = await waitForSubscribeUpdateMatchingPredicate(
      subscribe_duplex_stream,
      (data) => Boolean(data?.updateOneof?.transaction?.transaction),
      TEST_TIMEOUT,
    );

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

  }, TEST_TIMEOUT)

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

    subscribe_duplex_stream.write(request, (err) => {
      if (err) {
        console.error(`error writing to stream: ${err}`)
      }
    });

    const subscribe_update_response = await waitForSubscribeUpdateMatchingPredicate(
      subscribe_duplex_stream,
      (data) => Boolean(data?.updateOneof?.transaction?.transaction),
      TEST_TIMEOUT,
    );

    const tx_info = subscribe_update_response.updateOneof.transaction.transaction;
    const encoded = SubscribeUpdateTransactionInfo.encode(tx_info).finish();
    const decoded = SubscribeUpdateTransactionInfo.decode(encoded);

    expect(encoded).toBeInstanceOf(Uint8Array);
    expect(encoded.length).toBeGreaterThan(0);
    expect(decoded).toBeDefined();
    expect(decoded.signature).toBeDefined();
  }, TEST_TIMEOUT)
});
