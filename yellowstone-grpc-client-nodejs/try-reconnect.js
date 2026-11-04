// Tries subscribeWithReconnect() against TEST_ENDPOINT from .env. It connects through a local
// TCP proxy that cuts every connection every few seconds, so each cut shows a Disconnected
// event, and with replay, the partial banks coming back whole.
//
//   npm run build:dev                      # once, to build dist/ and the native binding
//   node try-reconnect.js                  # replay (the default), a cut every 10 s
//   node try-reconnect.js --no-replay --cut-every 5
//
// The proxy only works for plain http:// endpoints. With https://, the script connects
// directly and never cuts.

require("dotenv").config();
const net = require("node:net");
const sdk = require("./dist/cjs/index.js");

const Client = sdk.default;
const SYSVAR_OWNER = "Sysvar1111111111111111111111111111111111111";

function argValue(name) {
  const index = process.argv.indexOf(name);
  return index === -1 ? undefined : process.argv[index + 1];
}

/** Forwards local TCP connections to `target`; `cut()` drops every connection open now. */
function startProxy(target) {
  const url = new URL(target);
  const sockets = new Set();
  const server = net.createServer((client) => {
    const upstream = net.connect(Number(url.port || 80), url.hostname);
    for (const socket of [client, upstream]) {
      sockets.add(socket);
      socket.on("close", () => sockets.delete(socket));
      socket.on("error", () => {});
    }
    client.pipe(upstream).pipe(client);
  });
  return new Promise((resolve) =>
    server.listen(0, "127.0.0.1", () =>
      resolve({
        endpoint: `http://127.0.0.1:${server.address().port}`,
        cut() {
          for (const socket of sockets) socket.destroy();
        },
      }),
    ),
  );
}

async function main() {
  const target = process.env.TEST_ENDPOINT ?? "http://127.0.0.1:10000";
  const policy = process.argv.includes("--no-replay") ? "NoReplay" : "Replay";
  const cutEvery = Number(argValue("--cut-every") ?? 10);

  let endpoint = target;
  if (target.startsWith("http://")) {
    const proxy = await startProxy(target);
    endpoint = proxy.endpoint;
    setInterval(() => {
      console.log("✂  cutting the connection");
      proxy.cut();
    }, cutEvery * 1000).unref();
  } else {
    console.log("https endpoint: connecting directly, no cuts");
  }

  const client = new Client(endpoint, process.env.TEST_TOKEN, undefined, {
    policy,
    slotsBehind: 0,
    backoff: { initialIntervalMs: 100, multiplier: 2, maxRetries: 5 },
  });
  await client.connect();
  const stream = await client.subscribeWithReconnect({
    accounts: { sysvars: { owner: [SYSVAR_OWNER] } },
    blocksMeta: { blocks: {} },
    commitment: sdk.CommitmentLevel.PROCESSED,
  });
  console.log(
    `subscribed to ${target} with policy ${policy}, a cut every ${cutEvery} s`,
  );

  // What a consumer keeps: banks being built, and banks already complete (a replay resends them).
  const inProgress = new Map(); // "slot/bankId" -> sysvar writes so far
  const complete = new Set();
  const key = (slot, bankId) => `${slot}/${bankId}`;

  for await (const event of stream) {
    if (event.type === "Disconnected") {
      console.log(
        `DISCONNECTED: dropping ${event.partialBanks.length} partial bank(s)`,
        event.partialBanks.map((bank) => key(bank.slot, bank.bankId)),
        `reconnecting from slot ${event.replayFromSlot ?? "the live tip"}`,
      );
      for (const bank of event.partialBanks)
        inProgress.delete(key(bank.slot, bank.bankId));
      continue;
    }
    const { account, blockMeta } = event.update;
    if (account?.bankId !== undefined) {
      const bank = key(account.slot, account.bankId);
      if (!complete.has(bank))
        inProgress.set(bank, (inProgress.get(bank) ?? 0) + 1);
    } else if (blockMeta) {
      const bank = key(blockMeta.slot, blockMeta.bankId);
      if (complete.has(bank)) {
        console.log(`bank ${bank} resent by the replay, skipped`);
        continue;
      }
      complete.add(bank);
      console.log(
        `bank ${bank} complete: ${inProgress.get(bank) ?? 0} sysvar writes`,
      );
      inProgress.delete(bank);
    }
  }
}

main().catch((error) => {
  console.error("subscription failed:", error);
  process.exit(1);
});
