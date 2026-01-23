import Client from "../src"

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

  const waitForStreamClosed = (
    stream: NodeJS.ReadableStream,
    ignoreErrors: { value: boolean },
  ) =>
    new Promise<void>((resolve, reject) => {
      stream.on("error", (error) => {
        if (ignoreErrors.value) {
          resolve();
          return;
        }
        reject(error);
      });
      stream.on("end", () => {
        resolve();
      });
      stream.on("close", () => {
        resolve();
      });
    });

  // A helper function to check the Buffer structure repeatedly
  const expectBufferSchema = (bufferObject: any) => {
      expect(bufferObject).toEqual(
          expect.objectContaining({
              type: expect.any(String), // Should be 'Buffer'
              data: expect.any(Array),  // The array of bytes
          })
      );
      // Optionally, ensure the data array contains only numbers
      expect(bufferObject.data.every((item: any) => typeof item === 'number')).toBe(true);
  };

	test("account", async () => {
    let response: any;
    const stream = await client.subscribe();
    const request = {
      accounts: {
        client: {}
      },
      commitment: 2
    };

    stream.write(request, (err) => {
      if (err) {
        console.error(`error writing to stream: ${err}`)
      }
    });

    const ignoreErrors = { value: false };
    stream.once("data", (data) => {
      response = data;
      ignoreErrors.value = true;
      stream.end();
      stream.destroy();
    });

    const streamClosed = waitForStreamClosed(stream, ignoreErrors);

    await streamClosed;

    expect(response.filters).toEqual(["client"]);
    // We're doing it this way so we can bypass the Jest Globals vs Node Globals
    // type conflicts that makes expect(Date).toBeInstanceOf(Date) to fail.
    //
    // See issue here: https://github.com/jestjs/jest/issues/2549
    expect(Object.prototype.toString.call(response.createdAt)).toBe("[object Date]");
    expect(typeof response.account).toBe("object");
    expect(typeof response.account.slot).toBe("string");
    expect(typeof response.account.isStartup).toBe("boolean");
    expect(typeof response.account.account).toBe("object");

    const account = response.account.account;
    expect(account.pubkey).toBeInstanceOf(Buffer);
    expect(account.owner).toBeInstanceOf(Buffer);
    expect(account.data).toBeInstanceOf(Buffer);
    expect(typeof account.lamports).toBe("string");
    expect(typeof account.rentEpoch).toBe("string");
    expect(typeof account.writeVersion).toBe("string");
    expect(typeof account.executable).toBe("boolean");

	}, TEST_TIMEOUT);

	test("slot", async () => {
    let response: any;
    const stream = await client.subscribe();
    const request = {
      slots: {
        client: {}
      },
      commitment: 2
    };

    stream.write(request, (err) => {
      if (err) {
        console.error(`error writing to stream: ${err}`)
      }
    });

    const ignoreErrors = { value: false };
    stream.once("data", (data) => {
      response = data;
      ignoreErrors.value = true;
      stream.end();
      stream.destroy();
    });

    const streamClosed = waitForStreamClosed(stream, ignoreErrors);

    await streamClosed;

    expect(response.filters).toEqual(["client"]);
    // We're doing it this way so we can bypass the Jest Globals vs Node Globals
    // type conflicts that makes expect(Date).toBeInstanceOf(Date) to fail.
    //
    // See issue here: https://github.com/jestjs/jest/issues/2549
    expect(Object.prototype.toString.call(response.createdAt)).toBe("[object Date]");
    expect(typeof response.slot).toBe("object");
    expect(typeof response.slot.slot).toBe("string");
    expect(typeof response.slot.status).toBe("number");

	}, TEST_TIMEOUT);

	test("transaction", async () => {
    let response: any;
    const stream = await client.subscribe();
    const request = {
      transactions: {
        client: {},
      },
      commitment: 2
    };

    stream.write(request, (err) => {
      if (err) {
        console.error(`error writing to stream: ${err}`)
      }
    });

    const ignoreErrors = { value: false };
    stream.once("data", (data) => {
      response = data;
      ignoreErrors.value = true;
      stream.end();
      stream.destroy();
    });

    const streamClosed = waitForStreamClosed(stream, ignoreErrors);

    await streamClosed;

    expect(response.filters).toEqual(["client"]);
    // We're doing it this way so we can bypass the Jest Globals vs Node Globals
    // type conflicts that makes expect(Date).toBeInstanceOf(Date) to fail.
    //
    // See issue here: https://github.com/jestjs/jest/issues/2549
    expect(Object.prototype.toString.call(response.createdAt)).toBe("[object Date]");
    expect(typeof response.transaction).toBe("object");
    expect(typeof response.transaction.slot).toBe("string");

    const tx = response.transaction.transaction;
    expect(tx.signature).toBeInstanceOf(Buffer);
    expect(typeof tx.transaction).toBe("object");
    expect(typeof tx.meta).toBe("object");
    expect(typeof tx.index).toBe("string");
    expect(typeof tx.isVote).toBe("boolean");

    const txMeta = response.transaction.transaction.meta;
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

    const innerTx = response.transaction.transaction.transaction;
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

	test("block", async () => {
    let response: any;
    const stream = await client.subscribe();
    const request = {
      blocks: {
        client: {}
      },
      commitment: 2
    };

    stream.write(request, (err) => {
      if (err) {
        console.error(`error writing to stream: ${err}`)
      }
    });

    const ignoreErrors = { value: false };
    stream.once("data", (data) => {
      response = data;
      ignoreErrors.value = true;
      stream.end();
      stream.destroy();
    });

    const streamClosed = waitForStreamClosed(stream, ignoreErrors);

    await streamClosed;

    expect(response.filters).toEqual(["client"]);
    // We're doing it this way so we can bypass the Jest Globals vs Node Globals
    // type conflicts that makes expect(Date).toBeInstanceOf(Date) to fail.
    //
    // See issue here: https://github.com/jestjs/jest/issues/2549
    expect(Object.prototype.toString.call(response.createdAt)).toBe("[object Date]");
    expect(Object.prototype.toString.call(response.block.entries)).toBe("[object Array]");
    expect(Object.prototype.toString.call(response.block.accounts)).toBe("[object Array]");
    expect(Object.prototype.toString.call(response.block.transactions)).toBe("[object Array]");
    expect(typeof response.block).toBe("object");
    expect(typeof response.block.slot).toBe("string");
    expect(typeof response.block.blockhash).toBe("string");
    expect(typeof response.block.parentSlot).toBe("string");
    expect(typeof response.block.parentBlockhash).toBe("string");
    expect(typeof response.block.executedTransactionCount).toBe("string");
    expect(typeof response.block.updatedAccountCount).toBe("string");
    expect(typeof response.block.entriesCount).toBe("string");

    const rewards = response.block.rewards;
    expect(typeof rewards).toBe("object");
    expect(Object.prototype.toString.call(rewards.rewards)).toBe("[object Array]");

    const blockTime = response.block.blockTime;
    expect(typeof blockTime).toBe("object");
    expect(typeof blockTime.timestamp).toBe("string");

    const blockHeight = response.block.blockHeight;
    expect(typeof blockHeight).toBe("object");
    expect(typeof blockHeight.blockHeight).toBe("string");

	}, TEST_TIMEOUT);

	test("entry", async () => {
    let response: any;
    const stream = await client.subscribe();
    const request = {
      entry: {
        client: {}
      },
      commitment: 2
    };

    stream.write(request, (err) => {
      if (err) {
        console.error(`error writing to stream: ${err}`)
      }
    });

    const ignoreErrors = { value: false };
    stream.once("data", (data) => {
      response = data;
      ignoreErrors.value = true;
      stream.end();
      stream.destroy();
    });

    const streamClosed = waitForStreamClosed(stream, ignoreErrors);

    await streamClosed;

    expect(response.filters).toEqual(["client"]);
    // We're doing it this way so we can bypass the Jest Globals vs Node Globals
    // type conflicts that makes expect(Date).toBeInstanceOf(Date) to fail.
    //
    // See issue here: https://github.com/jestjs/jest/issues/2549
    expect(Object.prototype.toString.call(response.createdAt)).toBe("[object Date]");
    expect(typeof response.entry).toBe("object");
    expect(typeof response.entry.slot).toBe("string");
    expect(typeof response.entry.index).toBe("string");
    expect(typeof response.entry.numHashes).toBe("string");
    expect(typeof response.entry.executedTransactionCount).toBe("string");
    expect(typeof response.entry.startingTransactionIndex).toBe("string");
    expect(response.entry.hash).toBeInstanceOf(Buffer);

	}, TEST_TIMEOUT);

	test("ping", async () => {
    let response: any;
    const stream = await client.subscribe();
    const request = {
      ping: {
        id: 0
      },
      commitment: 2
    };

    stream.write(request, (err) => {
      if (err) {
        console.error(`error writing to stream: ${err}`)
      }
    });

    const ignoreErrors = { value: false };
    stream.once("data", (data) => {
      response = data;
      ignoreErrors.value = true;
      stream.end();
      stream.destroy();
    });

    const streamClosed = waitForStreamClosed(stream, ignoreErrors);

    await streamClosed;

    expect(response.filters).toEqual([]);
    // We're doing it this way so we can bypass the Jest Globals vs Node Globals
    // type conflicts that makes expect(Date).toBeInstanceOf(Date) to fail.
    //
    // See issue here: https://github.com/jestjs/jest/issues/2549
    expect(Object.prototype.toString.call(response.createdAt)).toBe("[object Date]");
    expect(typeof response.pong).toBe("object");
    expect(typeof response.pong.id).toBe("number");
    expect(response.pong.id).toEqual(0);

	}, TEST_TIMEOUT);

	test("blockMeta", async () => {
    let response: any;
    const stream = await client.subscribe();
    const request = {
      blocksMeta: {
        client: {},
      },
      commitment: 2
    };

    stream.write(request, (err) => {
      if (err) {
        console.error(`error writing to stream: ${err}`)
      }
    });

    const ignoreErrors = { value: false };
    stream.once("data", (data) => {
      response = data;
      ignoreErrors.value = true;
      stream.end();
      stream.destroy();
    });

    const streamClosed = waitForStreamClosed(stream, ignoreErrors);

    await streamClosed;

    expect(response.filters).toEqual(["client"]);
    // We're doing it this way so we can bypass the Jest Globals vs Node Globals
    // type conflicts that makes expect(Date).toBeInstanceOf(Date) to fail.
    //
    // See issue here: https://github.com/jestjs/jest/issues/2549
    expect(Object.prototype.toString.call(response.createdAt)).toBe("[object Date]");
    expect(typeof response.blockMeta).toBe("object");
    expect(typeof response.blockMeta.slot).toBe("string");
    expect(typeof response.blockMeta.blockhash).toBe("string");
    expect(typeof response.blockMeta.parentSlot).toBe("string");
    expect(typeof response.blockMeta.parentBlockhash).toBe("string");
    expect(typeof response.blockMeta.executedTransactionCount).toBe("string");
    expect(typeof response.blockMeta.entriesCount).toBe("string");

    const rewards = response.blockMeta.rewards;
    expect(typeof rewards).toBe("object");
    expect(Object.prototype.toString.call(rewards.rewards)).toBe("[object Array]");

    const blockTime = response.blockMeta.blockTime;
    expect(typeof blockTime).toBe("object");
    expect(typeof blockTime.timestamp).toBe("string");

    const blockHeight = response.blockMeta.blockHeight;
    expect(typeof blockHeight).toBe("object");
    expect(typeof blockHeight.blockHeight).toBe("string");

	}, TEST_TIMEOUT);
});
