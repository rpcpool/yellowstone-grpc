import Client, { CommitmentLevel } from "../dist/esm/index.js";

const endpoint = process.env.TEST_ENDPOINT;
if (!endpoint) {
  throw new Error("TEST_ENDPOINT is required");
}

const token = process.env.TEST_TOKEN || undefined;
const sampleMs = parsePositiveIntegerEnv("SAMPLE_MS", 30_000);
const warmupMs = parsePositiveIntegerEnv("WARMUP_MS", 15 * 60_000);
const durationMs = parsePositiveIntegerEnv("DURATION_MS", 7 * 60 * 60_000);
const maxDecodeBytes = parsePositiveIntegerEnv(
  "GRPC_MAX_DECODING_MESSAGE_SIZE",
  64 * 1024 * 1024,
);

const client = new Client(endpoint, token, {
  grpcMaxDecodingMessageSize: maxDecodeBytes,
});

await client.connect();
const stream = await client.subscribe();

stream.write(
  {
    accounts: {},
    slots: {},
    blocks: {},
    blocksMeta: {},
    entry: {},
    transactionsStatus: {},
    accountsDataSlice: [],
    transactions: {
      all: {
        accountInclude: [],
        accountExclude: [],
        accountRequired: [],
        vote: true,
        failed: true,
      },
    },
    commitment: CommitmentLevel.CONFIRMED,
  },
  (error) => {
    if (error) {
      console.error(`write error: ${error.message}`);
    }
  },
);

let txs = 0;
const samples = [];
const startedAt = Date.now();

stream.on("data", (update) => {
  if (!update.ping && !update.pong && update.transaction) {
    txs += 1;
  }
});

stream.on("error", (error) => {
  console.error(`stream error: ${error.message}`);
});

const timer = setInterval(() => {
  const now = Date.now();
  const elapsedMs = now - startedAt;
  const memory = process.memoryUsage();
  const stats = stream.nativeStats();
  const lagMessages = stats.updatesEnqueued - stats.updatesDequeued;

  if (elapsedMs >= warmupMs) {
    samples.push({
      elapsedMs,
      rss: memory.rss,
      queuedBytes: stats.queuedBytes,
    });
  }

  const rssSlope = slopePerHour(samples, "rss");
  const queuedSlope = slopePerHour(samples, "queuedBytes");

  console.log(
    [
      `t=${formatMinutes(elapsedMs)}`,
      `rss=${formatGb(memory.rss)}`,
      `heap=${formatMb(memory.heapUsed)}`,
      `external=${formatMb(memory.external)}`,
      `queued=${formatMb(stats.queuedBytes)}`,
      `queuedMsgs=${stats.queuedMessages}`,
      `maxQueued=${formatMb(stats.maxQueuedBytes)}`,
      `lag=${lagMessages}`,
      `enq=${stats.updatesEnqueued}`,
      `deq=${stats.updatesDequeued}`,
      `grpc=${stats.grpcUpdatesReceived}`,
      `reads=${stats.readCalls}`,
      `txs=${txs}`,
      `rssSlope=${formatGb(rssSlope)}/h`,
      `queuedSlope=${formatGb(queuedSlope)}/h`,
    ].join("  "),
  );
}, sampleMs);

const shutdown = () => {
  clearInterval(timer);
  stream.destroy();
};

process.once("SIGINT", () => {
  shutdown();
  process.exit(130);
});

process.once("SIGTERM", () => {
  shutdown();
  process.exit(143);
});

if (durationMs > 0) {
  setTimeout(() => {
    shutdown();
    printSummary(samples);
    process.exit(0);
  }, durationMs);
}

function parsePositiveIntegerEnv(name, fallback) {
  const raw = process.env[name];
  if (raw == null || raw === "") {
    return fallback;
  }
  const parsed = Number(raw);
  if (!Number.isFinite(parsed) || parsed < 0) {
    throw new Error(`${name} must be a non-negative number`);
  }
  return Math.trunc(parsed);
}

function slopePerHour(rows, key) {
  if (rows.length < 2) {
    return 0;
  }
  const first = rows[0];
  const last = rows[rows.length - 1];
  const hours = (last.elapsedMs - first.elapsedMs) / 3_600_000;
  if (hours <= 0) {
    return 0;
  }
  return (last[key] - first[key]) / hours;
}

function printSummary(rows) {
  const rssSlope = slopePerHour(rows, "rss");
  const queuedSlope = slopePerHour(rows, "queuedBytes");
  console.log(
    [
      "summary",
      `samples=${rows.length}`,
      `rssSlope=${formatGb(rssSlope)}/h`,
      `queuedSlope=${formatGb(queuedSlope)}/h`,
    ].join("  "),
  );
}

function formatMinutes(ms) {
  return `${Math.round(ms / 60_000)}m`;
}

function formatMb(bytes) {
  return `${(bytes / 1e6).toFixed(0)}MB`;
}

function formatGb(bytes) {
  return `${(bytes / 1e9).toFixed(2)}GB`;
}
