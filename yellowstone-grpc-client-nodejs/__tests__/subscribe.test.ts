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

    stream.once("data", (data) => {
      response = data;
      stream.end();
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

    stream.once("data", (data) => {
      response = data;
      stream.end();
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

    stream.once("data", (data) => {
      response = data;
      stream.end();
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
});
