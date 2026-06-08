import { inspect } from "node:util";
import Client, {
  CommitmentLevel,
  CompressedAccountFilterSet,
  SubscribeDeshredRequest,
  SubscribeRequest,
  SubscribeRequestFilterAccountsFilter,
  SubscribeRequestFilterAccountsFilterLamports,
  txDeshredEncode,
  txEncode,
  txErrDecode,
} from "@triton-one/yellowstone-grpc";
import type { ReconnectOptions } from "@triton-one/yellowstone-grpc";

async function main() {
  const args = await parseCommandLineArgs();
  const reconnectOptions = buildReconnectOptions(args);

  // Open connection.
  const client = new Client(
    args.endpoint,
    args.xToken,
    {
      // "grpc.max_receive_message_length": 64 * 1024 * 1024, // 64MiB
      grpcMaxDecodingMessageSize: 64 * 1024 * 1024,
    },
    reconnectOptions,
  );

  await client.connect();

  const commitment = parseCommitmentLevel(args.commitment);

  // Execute a requested command
  switch (args["_"][0]) {
    case "subscribe-replay-info":
      console.log("response: " + inspect(await client.subscribeReplayInfo()));
      break;

    case "ping":
      console.log("response: " + inspect(await client.ping(1)));
      break;

    case "get-version":
      console.log("response: " + inspect(await client.getVersion()));
      break;

    case "get-slot":
      console.log("response: " + inspect(await client.getSlot(commitment)));
      break;

    case "get-block-height":
      console.log("response: " + inspect(await client.getBlockHeight(commitment)));
      break;

    case "get-latest-blockhash":
      console.log("response: " + inspect(await client.getLatestBlockhash(commitment)));
      break;

    case "is-blockhash-valid":
      console.log(
        "response: " + inspect(await client.isBlockhashValid(String(args.blockhash))),
      );
      break;

    case "subscribe":
      await subscribeCommand(client, args);
      break;

    case "subscribeDeshred":
      await subscribeDeshredCommand(client, args);
      break;

    default:
      console.error(
        `Unknown command: ${args["_"]}. Use "--help" for a list of supported commands.`,
      );
      break;
  }
}

function parseCommitmentLevel(commitment: string | undefined) {
  if (!commitment) {
    return;
  }
  const typedCommitment =
    commitment.toUpperCase() as keyof typeof CommitmentLevel;
  return CommitmentLevel[typedCommitment];
}

function buildReconnectOptions(args): ReconnectOptions | undefined {
  if (!args.autoreconnect) {
    return undefined;
  }

  return {
    enabled: true,
    backoff: {
      initialIntervalMs: args.autoreconnectInitialIntervalMs,
      multiplier: args.autoreconnectMultiplier,
      maxRetries: args.autoreconnectMaxRetries,
    },
    slotRetention: args.autoreconnectSlotRetention,
  };
}

function stringArray(values: unknown[] | unknown | undefined): string[] {
  if (values === undefined) {
    return [];
  }
  return (Array.isArray(values) ? values : [values]).map((value) =>
    String(value),
  );
}

function parsePair(value: unknown, separator: string, optionName: string) {
  const spec = String(value);
  const separatorIndex = spec.indexOf(separator);
  if (separatorIndex <= 0 || separatorIndex === spec.length - 1) {
    throw new Error(`invalid ${optionName}`);
  }

  return [
    spec.slice(0, separatorIndex),
    spec.slice(separatorIndex + separator.length),
  ] as const;
}

function buildCompressedAccountSet(
  pubkeys: unknown[] | undefined,
  maxCapacity: number | undefined,
  optionName: string,
): CompressedAccountFilterSet {
  const trackedPubkeys = stringArray(pubkeys);
  if (trackedPubkeys.length === 0) {
    throw new Error(`${optionName} requires at least one pubkey`);
  }

  const accounts = new CompressedAccountFilterSet(
    maxCapacity ?? trackedPubkeys.length,
  );
  for (const pubkey of trackedPubkeys) {
    accounts.insert(pubkey);
  }

  return accounts;
}

type BuiltSubscribeRequest = {
  request: SubscribeRequest;
  accountCompressedFilter?: CompressedAccountFilterSet;
};

function buildSubscribeRequest(args): BuiltSubscribeRequest {
  const request: SubscribeRequest = {
    accounts: {},
    slots: {},
    transactions: {},
    transactionsStatus: {},
    entry: {},
    blocks: {},
    blocksMeta: {},
    commitment: parseCommitmentLevel(args.commitment),
    accountsDataSlice: [],
    ping: undefined,
  };
  let accountCompressedFilter: CompressedAccountFilterSet | undefined;

  if (args.accountsCompressed && !args.accounts) {
    throw new Error("--accounts-compressed requires --accounts");
  }

  if (args.blocksCompressed && !args.blocks) {
    throw new Error("--blocks-compressed requires --blocks");
  }

  if (args.accountsCompressed) {
    accountCompressedFilter = buildCompressedAccountSet(
      args.accountsAccount,
      args.accountsCompressedCapacity,
      "--accounts-compressed",
    );
  }

  const blockCompressedFilter = args.blocksCompressed
    ? buildCompressedAccountSet(
        args.blocksAccountInclude,
        args.blocksCompressedCapacity,
        "--blocks-compressed",
      )
    : undefined;

  if (args.accounts) {
    const filters: SubscribeRequestFilterAccountsFilter[] = [];

    if (args.accountsMemcmp) {
      for (const filter of stringArray(args.accountsMemcmp)) {
        const [offset, data] = parsePair(filter, ",", "memcmp");
        filters.push({
          memcmp: { offset, base58: data.trim() },
        });
      }
    }

    if (args.accountsTokenaccountstate) {
      filters.push({
        tokenAccountState: args.accountsTokenaccountstate,
      });
    }

    if (args.accountsDatasize) {
      filters.push({ datasize: String(args.accountsDatasize) });
    }

    if (args.accountsLamports) {
      for (const filter of stringArray(args.accountsLamports)) {
        const [cmp, value] = parsePair(filter, ":", "lamports");
        let lamports: SubscribeRequestFilterAccountsFilterLamports = {};
        switch (cmp) {
          case "eq": {
            lamports.eq = value;
            break;
          }
          case "ne": {
            lamports.ne = value;
            break;
          }
          case "lt": {
            lamports.lt = value;
            break;
          }
          case "gt": {
            lamports.gt = value;
            break;
          }
          default:
            throw new Error("invalid lamports cmp");
        }
        filters.push({
          lamports,
        });
      }
    }

    request.accounts.client = {
      ...(accountCompressedFilter?.toAccountFilter() ?? {}),
      account: accountCompressedFilter ? [] : stringArray(args.accountsAccount),
      owner: stringArray(args.accountsOwner),
      filters,
      nonemptyTxnSignature: args.accountsNonemptytxnsignature,
    };

  }

  if (args.slots) {
    request.slots.client = {
      filterByCommitment: args.slotsFilterByCommitment,
    };
  }

  if (args.transactions) {
    request.transactions.client = {
      vote: args.transactionsVote,
      failed: args.transactionsFailed,
      signature: args.transactionsSignature,
      accountInclude: args.transactionsAccountInclude,
      accountExclude: args.transactionsAccountExclude,
      accountRequired: args.transactionsAccountRequired,
    };
  }

  if (args.transactionsStatus) {
    request.transactionsStatus.client = {
      vote: args.transactionsStatusVote,
      failed: args.transactionsStatusFailed,
      signature: args.transactionsStatusSignature,
      accountInclude: args.transactionsStatusAccountInclude,
      accountExclude: args.transactionsStatusAccountExclude,
      accountRequired: args.transactionsStatusAccountRequired,
    };
  }

  if (args.entry) {
    request.entry.client = {};
  }

  if (args.blocks) {
    request.blocks.client = {
      ...(blockCompressedFilter?.toBlockFilter() ?? {}),
      accountInclude: blockCompressedFilter
        ? []
        : stringArray(args.blocksAccountInclude),
      includeTransactions: args.blocksIncludeTransactions,
      includeAccounts: args.blocksIncludeAccounts,
      includeEntries: args.blocksIncludeEntries,
    };

  }

  if (args.blocksMeta) {
    request.blocksMeta.client = {};
  }

  if (args.accountsDataslice) {
    for (const filter of stringArray(args.accountsDataslice)) {
      const [offset, length] = parsePair(filter, ",", "data slice");
      request.accountsDataSlice.push({
        offset,
        length,
      });
    }
  }

  if (args.ping) {
    request.ping = { id: args.ping };
  }

  return { request, accountCompressedFilter };
}

async function subscribeCommand(client: Client, args) {
  // Create subscribe request based on provided arguments.
  const { request, accountCompressedFilter } = buildSubscribeRequest(args);

  // Subscribe for events. When auto-reconnect is enabled, pass the initial
  // request at stream creation so reconnects can resume that subscription.
  const stream = args.autoreconnect
    ? await client.subscribe(request)
    : await client.subscribe();

  // Create `error` / `end` handler
  const streamClosed = new Promise<void>((resolve, reject) => {
    stream.on("error", (error) => {
      reject(error);
      stream.end();
    });
    stream.on("end", () => {
      resolve();
    });
    stream.on("close", () => {
      resolve();
    });
  });

  // Handle updates
  stream.on("data", (data) => {
    if (
      data.transaction &&
      (args.transactionsParsed || args.transactionsDecodeErr)
    ) {
      const slot = data.transaction.slot;
      const message = data.transaction.transaction;
      if (args.transactionsParsed) {
        const tx = txEncode.encode(message, txEncode.encoding.Json, 255, true);
        console.log(
          `TX filters: ${data.filters}, slot#${slot}, tx: ${JSON.stringify(tx)}`,
        );
      }
      if (message.meta.err && args.transactionsDecodeErr) {
        const err = txErrDecode.decode(message.meta.err.err);
        console.log(
          `TX filters: ${data.filters}, slot#${slot}, err: ${inspect(err)}}`,
        );
      }
      return;
    }

    if (accountCompressedFilter && data.account) {
      const pubkey = data.account.account?.pubkey;
      if (!pubkey || !accountCompressedFilter.contains(pubkey)) {
        return;
      }
    }

    console.log("data", data);
  });

  // Send subscribe request
  if (!args.autoreconnect) {
    await new Promise<void>((resolve, reject) => {
      stream.write(request, (err) => {
        if (err === null || err === undefined) {
          resolve();
        } else {
          reject(err);
        }
      });
    }).catch((reason) => {
      console.error(reason);
      throw reason;
    });
  }

  await streamClosed;
}

async function subscribeDeshredCommand(client: Client, args) {
  const stream = await client.subscribeDeshred();

  const streamClosed = new Promise<void>((resolve, reject) => {
    stream.on("error", (error) => {
      reject(error);
      stream.end();
    });
    stream.on("end", () => {
      resolve();
    });
    stream.on("close", () => {
      resolve();
    });
  });

  stream.on("data", (data) => {
    const txMessage = data.deshredTransaction?.transaction;

    if (data.deshredTransaction && args.deshredParsed) {
      if (!txMessage?.transaction) {
        console.log("data", data);
        return;
      }

      try {
        const tx = txDeshredEncode.encode(
          txMessage,
          txDeshredEncode.encoding.JsonParsed,
        );
        console.log(
          `DESHRED filters: ${data.filters}, slot#${data.deshredTransaction.slot}, tx: ${JSON.stringify(tx)}`,
        );
      } catch (error) {
        console.error(
          `failed to parse deshred tx (slot#${data.deshredTransaction.slot})`,
          error,
        );
      }
      return;
    }

    console.log("data", data);
  });

  const request: SubscribeDeshredRequest = {
    deshredTransactions: {
      client: {
        vote: args.deshredVote,
        accountInclude: args.deshredAccountInclude,
        accountExclude: args.deshredAccountExclude,
        accountRequired: args.deshredAccountRequired,
      },
    },
    ping: args.ping ? { id: args.ping } : undefined,
    slots: {},
  };

  await new Promise<void>((resolve, reject) => {
    stream.write(request, (err) => {
      if (err === null || err === undefined) {
        resolve();
      } else {
        reject(err);
      }
    });
  }).catch((reason) => {
    console.error(reason);
    throw reason;
  });

  await streamClosed;
}

async function parseCommandLineArgs() {
  const { default: yargs } = await import("yargs");

  return yargs(process.argv.slice(2))
    .options({
      endpoint: {
        alias: "e",
        default: "http://localhost:10000",
        describe: "gRPC endpoint",
        type: "string",
      },
      "x-token": {
        describe: "token for auth, can be used only with ssl",
        type: "string",
      },
      commitment: {
        describe: "commitment level",
        choices: ["processed", "confirmed", "finalized"],
      },
      autoreconnect: {
        default: false,
        describe: "enable auto-reconnect for standard subscribe streams",
        type: "boolean",
      },
      "autoreconnect-initial-interval-ms": {
        default: 10,
        describe: "auto-reconnect first retry delay in milliseconds",
        type: "number",
      },
      "autoreconnect-multiplier": {
        default: 2,
        describe: "auto-reconnect exponential backoff multiplier",
        type: "number",
      },
      "autoreconnect-max-retries": {
        default: 3,
        describe: "auto-reconnect retry count per reconnect attempt",
        type: "number",
      },
      "autoreconnect-slot-retention": {
        default: 250,
        describe: "slots retained for auto-reconnect deduplication",
        type: "number",
      },
    })
    .command("subscribe-replay-info", "get subscribe replay info")
    .command("ping", "single ping of the RPC server")
    .command("get-version", "get the server version")
    .command("get-latest-blockhash", "get the latest block hash")
    .command("get-block-height", "get the current block height")
    .command("get-slot", "get the current slot")
    .command(
      "is-blockhash-valid",
      "check the validity of a given block hash",
      (yargs) => {
        return yargs.options({
          blockhash: {
            type: "string",
            demandOption: true,
          },
        });
      },
    )
    .command("subscribe", "subscribe to events", (yargs) => {
      return yargs.options({
        accounts: {
          default: false,
          describe: "subscribe on accounts updates",
          type: "boolean",
        },
        "accounts-account": {
          default: [],
          describe: "filter by account pubkey",
          type: "array",
        },
        "accounts-compressed": {
          default: false,
          describe:
            "send --accounts-account pubkeys as a compressed account filter",
          type: "boolean",
        },
        "accounts-compressed-capacity": {
          describe:
            "max capacity for --accounts-compressed; defaults to number of --accounts-account values",
          type: "number",
        },
        "accounts-owner": {
          default: [],
          describe: "filter by owner pubkey",
          type: "array",
        },
        "accounts-memcmp": {
          default: [],
          describe:
            "filter by offset and data, format: `offset,data in base58`",
          type: "array",
        },
        "accounts-datasize": {
          default: 0,
          describe: "filter by data size",
          type: "number",
        },
        "accounts-tokenaccountstate": {
          default: false,
          describe: "filter valid token accounts",
          type: "boolean",
        },
        "accounts-lamports": {
          default: [],
          describe:
            "filter by lamports, format: `eq:42` / `ne:42` / `lt:42` / `gt:42`",
          type: "array",
        },
        "accounts-nonemptytxnsignature": {
          description: "filter by presence of field txn_signature",
          type: "boolean",
        },
        "accounts-dataslice": {
          default: [],
          describe:
            "receive only part of updated data account, format: `offset,size`",
          type: "array",
        },
        slots: {
          default: false,
          describe: "subscribe on slots updates",
          type: "boolean",
        },
        "slots-filter-by-commitment": {
          default: false,
          describe: "filter slot messages by commitment",
          type: "boolean",
        },
        transactions: {
          default: false,
          describe: "subscribe on transactions updates",
          type: "boolean",
        },
        "transactions-vote": {
          description: "filter vote transactions",
          type: "boolean",
        },
        "transactions-failed": {
          description: "filter failed transactions",
          type: "boolean",
        },
        "transactions-signature": {
          description: "filter by transaction signature",
          type: "string",
        },
        "transactions-account-include": {
          default: [],
          description: "filter included account in transactions",
          type: "array",
        },
        "transactions-account-exclude": {
          default: [],
          description: "filter excluded account in transactions",
          type: "array",
        },
        "transactions-account-required": {
          default: [],
          description: "filter required account in transactions",
          type: "array",
        },
        "transactions-parsed": {
          default: false,
          describe: "parse transaction to json",
          type: "boolean",
        },
        "transactions-decode-err": {
          default: false,
          describe: "decode transactions errors",
          type: "boolean",
        },
        "transactions-status": {
          default: false,
          describe: "subscribe on transactionsStatus updates",
          type: "boolean",
        },
        "transactions-status-vote": {
          description: "filter vote transactions",
          type: "boolean",
        },
        "transactions-status-failed": {
          description: "filter failed transactions",
          type: "boolean",
        },
        "transactions-status-signature": {
          description: "filter by transaction signature",
          type: "string",
        },
        "transactions-status-account-include": {
          default: [],
          description: "filter included account in transactions",
          type: "array",
        },
        "transactions-status-account-exclude": {
          default: [],
          description: "filter excluded account in transactions",
          type: "array",
        },
        "transactions-status-account-required": {
          default: [],
          description: "filter required account in transactions",
          type: "array",
        },
        entry: {
          default: false,
          description: "subscribe on entry updates",
          type: "boolean",
        },
        blocks: {
          default: false,
          description: "subscribe on block updates",
          type: "boolean",
        },
        "blocks-account-include": {
          default: [],
          description: "filter included account in transactions",
          type: "array",
        },
        "blocks-compressed": {
          default: false,
          description:
            "send --blocks-account-include pubkeys as a compressed account filter",
          type: "boolean",
        },
        "blocks-compressed-capacity": {
          description:
            "max capacity for --blocks-compressed; defaults to number of --blocks-account-include values",
          type: "number",
        },
        "blocks-include-transactions": {
          default: false,
          description: "include transactions to block messsage",
          type: "boolean",
        },
        "blocks-include-accounts": {
          default: false,
          description: "include accounts to block message",
          type: "boolean",
        },
        "blocks-include-entries": {
          default: false,
          description: "include entries to block message",
          type: "boolean",
        },
        "blocks-meta": {
          default: false,
          description: "subscribe on block meta updates (without transactions)",
          type: "boolean",
        },
        ping: {
          default: undefined,
          description: "send ping request in subscribe",
          type: "number",
        },
      });
    })
    .command(
      "subscribeDeshred",
      "subscribe to deshred transactions",
      (yargs) => {
        return yargs.options({
          "deshred-parsed": {
            default: false,
            describe: "parse deshred transactions using txDeshredEncode",
            type: "boolean",
          },
          "deshred-vote": {
            description: "filter vote transactions",
            type: "boolean",
          },
          "deshred-account-include": {
            default: [],
            description:
              "filter included accounts in deshred transactions (static + ALT)",
            type: "array",
          },
          "deshred-account-exclude": {
            default: [],
            description:
              "filter excluded accounts in deshred transactions (static + ALT)",
            type: "array",
          },
          "deshred-account-required": {
            default: [],
            description:
              "filter required accounts in deshred transactions (static + ALT)",
            type: "array",
          },
          ping: {
            default: undefined,
            description: "send ping request in subscribeDeshred",
            type: "number",
          },
        });
      },
    )
    .demandCommand(1)
    .help()
    .parseSync();
}

main();
