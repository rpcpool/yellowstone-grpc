import Client from "../src"

describe("stream lifecycle and subscription pattern tests", () => {
  const TEST_TIMEOUT = 100000;

  // .env
  const {
    TEST_ENDPOINT: endpoint,
    TEST_TOKEN: xToken
  } = process.env;

  // Use options sensible defaults.
  const channelOptions = {};
  const client = new Client(endpoint, xToken, channelOptions);

  test("stream.destroyed is true after destroy()", async () => {
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

    stream.once("data", () => {
      stream.destroy();
    });

    const streamClosed = new Promise<void>((resolve, reject) => {
      stream.on("error", (error) => {
        reject(error);
      });
      stream.on("close", () => {
        resolve();
      });
    });

    await streamClosed;

    expect(stream.destroyed).toBe(true);
  }, TEST_TIMEOUT);

  test("stream emits 'finish' event and writableEnded is true after end()", async () => {
    const stream = await client.subscribe();
    const request = {
      ping: {
        id: 1
      },
      commitment: 2
    };

    let finishFired = false;

    stream.write(request, (err) => {
      if (err) {
        console.error(`error writing to stream: ${err}`)
      }
    });

    stream.once("data", () => {
      stream.end();
    });

    stream.on("finish", () => {
      finishFired = true;
      // Destroy to close the readable side as well, so the test can complete.
      stream.destroy();
    });

    const streamClosed = new Promise<void>((resolve, reject) => {
      stream.on("error", (error) => {
        reject(error);
      });
      stream.on("close", () => {
        resolve();
      });
    });

    await streamClosed;

    expect(finishFired).toBe(true);
    expect(stream.writableEnded).toBe(true);
  }, TEST_TIMEOUT);

  test("stream emits 'finish' before 'close' when closed via end()", async () => {
    const stream = await client.subscribe();
    const request = {
      ping: {
        id: 2
      },
      commitment: 2
    };

    const events: string[] = [];

    stream.write(request, (err) => {
      if (err) {
        console.error(`error writing to stream: ${err}`)
      }
    });

    stream.on("finish", () => events.push("finish"));
    stream.on("close", () => events.push("close"));

    stream.once("data", () => {
      stream.end();
    });

    // After end(), destroy to ensure readable side also closes.
    stream.on("finish", () => {
      stream.destroy();
    });

    const streamClosed = new Promise<void>((resolve, reject) => {
      stream.on("error", (error) => {
        reject(error);
      });
      stream.on("close", () => {
        resolve();
      });
    });

    await streamClosed;

    expect(events).toContain("finish");
    expect(events).toContain("close");
    const finishIndex = events.indexOf("finish");
    const closeIndex = events.indexOf("close");
    expect(finishIndex).toBeLessThan(closeIndex);
  }, TEST_TIMEOUT);

  test("stream can receive multiple consecutive messages from same subscription", async () => {
    const stream = await client.subscribe();
    const request = {
      slots: {
        client: {}
      },
      commitment: 2
    };

    const targetCount = 3;
    const responses: any[] = [];

    stream.write(request, (err) => {
      if (err) {
        console.error(`error writing to stream: ${err}`)
      }
    });

    const streamClosed = new Promise<void>((resolve, reject) => {
      stream.on("data", (data) => {
        responses.push(data);
        if (responses.length >= targetCount) {
          stream.destroy();
        }
      });
      stream.on("error", (error) => {
        reject(error);
      });
      stream.on("close", () => {
        resolve();
      });
    });

    await streamClosed;

    expect(responses.length).toBeGreaterThanOrEqual(targetCount);
    for (const response of responses) {
      expect(typeof response.slot).toBe("object");
      expect(typeof response.slot.slot).toBe("bigint");
      expect(Object.prototype.toString.call(response.createdAt)).toBe("[object Date]");
    }
  }, TEST_TIMEOUT);

  test("can update the subscription by writing a new request mid-stream", async () => {
    const stream = await client.subscribe();

    // First request: subscribe to slots
    const slotRequest = {
      slots: {
        client: {}
      },
      commitment: 2
    };

    // Second request: send a ping to get a pong response
    const pingRequest = {
      ping: {
        id: 77
      },
      commitment: 2
    };

    let slotResponse: any;
    let pingResponse: any;
    let receivedFirstMessage = false;

    stream.write(slotRequest, (err) => {
      if (err) {
        console.error(`error writing to stream: ${err}`)
      }
    });

    const streamClosed = new Promise<void>((resolve, reject) => {
      stream.on("data", (data) => {
        if (!receivedFirstMessage) {
          receivedFirstMessage = true;
          slotResponse = data;
          // Write a new request to change the subscription
          stream.write(pingRequest, (err) => {
            if (err) {
              console.error(`error writing ping to stream: ${err}`)
            }
          });
        } else if (data.pong !== undefined) {
          pingResponse = data;
          stream.destroy();
        }
      });
      stream.on("error", (error) => {
        reject(error);
      });
      stream.on("close", () => {
        resolve();
      });
    });

    await streamClosed;

    expect(slotResponse).toBeDefined();
    expect(typeof slotResponse.slot).toBe("object");
    expect(typeof slotResponse.slot.slot).toBe("bigint");

    expect(pingResponse).toBeDefined();
    expect(typeof pingResponse.pong).toBe("object");
    expect(pingResponse.pong.id).toBe(77);
  }, TEST_TIMEOUT);

  test("multiple concurrent subscriptions from the same client work independently", async () => {
    const [stream1, stream2] = await Promise.all([
      client.subscribe(),
      client.subscribe()
    ]);

    const slotRequest = {
      slots: {
        client: {}
      },
      commitment: 2
    };

    stream1.write(slotRequest, (err) => {
      if (err) console.error(`stream1 write error: ${err}`)
    });
    stream2.write(slotRequest, (err) => {
      if (err) console.error(`stream2 write error: ${err}`)
    });

    let response1: any;
    let response2: any;

    const promise1 = new Promise<void>((resolve, reject) => {
      stream1.once("data", (data) => {
        response1 = data;
        stream1.destroy();
      });
      stream1.on("error", (error) => {
        reject(error);
      });
      stream1.on("close", () => {
        resolve();
      });
    });

    const promise2 = new Promise<void>((resolve, reject) => {
      stream2.once("data", (data) => {
        response2 = data;
        stream2.destroy();
      });
      stream2.on("error", (error) => {
        reject(error);
      });
      stream2.on("close", () => {
        resolve();
      });
    });

    await Promise.all([promise1, promise2]);

    expect(response1).toBeDefined();
    expect(typeof response1.slot.slot).toBe("bigint");
    expect(response2).toBeDefined();
    expect(typeof response2.slot.slot).toBe("bigint");
  }, TEST_TIMEOUT);

  test("stream emits 'error' event when destroyed with an error argument", async () => {
    const stream = await client.subscribe();
    const request = {
      slots: {
        client: {}
      },
      commitment: 2
    };

    const testError = new Error("intentional test destroy error");
    let capturedError: Error | null = null;

    stream.write(request, (err) => {
      if (err) {
        console.error(`error writing to stream: ${err}`)
      }
    });

    stream.once("data", () => {
      stream.destroy(testError);
    });

    stream.on("error", (err) => {
      capturedError = err;
    });

    const streamClosed = new Promise<void>((resolve) => {
      stream.on("close", () => {
        resolve();
      });
    });

    await streamClosed;

    expect(stream.destroyed).toBe(true);
    expect(capturedError).toBe(testError);
  }, TEST_TIMEOUT);
});
