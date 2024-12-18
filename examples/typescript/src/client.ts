import yargs from "yargs";
import { inspect } from "node:util";
import Client, {
  CommitmentLevel,
  createGrpcClient,
  FumaroleSDKClient,
  FumaroleSubscribeRequest,
  SubscribeRequest,
  SubscribeRequestFilterAccountsFilter,
  SubscribeRequestFilterAccountsFilterLamports,
  YellowstoneGrpcClientConfig,
  YellowstoneGrpcClients,
  SubscribeUpdateTransactionInfo,
  txEncode,
  txErrDecode,
} from "@triton-one/yellowstone-grpc";
import { EventSubscriptionPolicy, FumaroleClient, InitialOffsetPolicy } from "@triton-one/yellowstone-grpc/dist/grpc/fumarole";

async function main() {
  const args = parseCommandLineArgs();

  const config: YellowstoneGrpcClientConfig = {
    endpoint: args.endpoint,
    xToken: args.xToken,
    channelOptions: {
      "grpc.max_receive_message_length": 1024 * 1024 * 64, // 64MiB
    }
  }

  // Open connection to gRPC server

  // Connections can be opened via:
  // 1. Direct client initialization
  // 2. Helper function
  // There is no difference between these, the helper function is added to make migrations seamless
  const dragonsMouthClient = new Client(args.endpoint, args.xToken, {
    "grpc.max_receive_message_length": 1024 * 1024 * 64, // 64MiB
  });
  // const dragonsMouthClient = createGrpcClient(YellowstoneGrpcClients.DragonsMouth, config)

  const fumaroleSubscriptionId = args.subscriptionId || crypto.randomUUID()

  // const fumaroleClient = new FumaroleSDKClient(args.endpoint, args.xToken, {
  //   "grpc.max_receive_message_length": 1024 * 1024 * 64, // 64MiB
  // }, fumaroleSubscriptionId);
  const fumaroleClient = createGrpcClient(YellowstoneGrpcClients.Fumarole, config, fumaroleSubscriptionId)

  const commitment = parseCommitmentLevel(args.commitment);

  // Execute a requested command
  switch (args["_"][0]) {
    case "ping":
      console.log("response: " + (await dragonsMouthClient.ping(1)));
      break;

    case "get-version":
      console.log("response: " + (await dragonsMouthClient.getVersion()));
      break;

    case "get-slot":
      console.log("response: " + (await dragonsMouthClient.getSlot(commitment)));
      break;

    case "get-block-height":
      console.log("response: " + (await dragonsMouthClient.getBlockHeight(commitment)));
      break;

    case "get-latest-blockhash":
      console.log("response: ", await dragonsMouthClient.getLatestBlockhash(commitment));
      break;

    case "is-blockhash-valid":
      console.log("response: ", await dragonsMouthClient.isBlockhashValid(args.blockhash));
      break;

    case "subscribe":
      await subscribeCommand(dragonsMouthClient, args);
      break;

    case "fumarole-subscribe":
      await fumaroleSubscribeCommand(fumaroleClient, args)
      break;

    case "fumarole-create-consumer-group":
      console.log(await fumaroleClient.createConsumerGroup({
        commitmentLevel: CommitmentLevel.CONFIRMED,
        consumerGroupLabel: args.consumerGroupLabel,
        eventSubscriptionPolicy: EventSubscriptionPolicy.BOTH,
        initialOffsetPolicy: InitialOffsetPolicy.LATEST
      }))
      break;

    case "fumarole-create-subscription-id":
      console.log(crypto.randomUUID())
      break;

    case "fumarole-get-slot-lag-info":
      const slotLagInfo = await fumaroleClient.getSlotLagInfo({ consumerGroupLabel: args.consumerGroupLabel })
      console.log(slotLagInfo);
      break;

    default:
      console.error(
        `Unknown command: ${args["_"]}. Use "--help" for a list of supported commands.`
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

async function fumaroleSubscribeCommand(client: FumaroleSDKClient, args) {
  const request: FumaroleSubscribeRequest = {
    accounts: {
      account: args.accountsAccount,
      owner: []
    },
    consumerGroupLabel: args.consumerGroupLabel,
    consumerId: 0
  };

  // Subscribe for events
  const stream = await client.subscribe();

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
    console.log(data);
  });

  // Send subscribe request
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

async function subscribeCommand(client, args) {
  // Subscribe for events
  const stream = await client.subscribe();

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
          `TX filters: ${data.filters}, slot#${slot}, tx: ${JSON.stringify(tx)}`
        );
      }
      if (message.meta.err && args.transactionsDecodeErr) {
        const err = txErrDecode.decode(message.meta.err.err);
        console.log(
          `TX filters: ${data.filters}, slot#${slot}, err: ${inspect(err)}}`
        );
      }
      return;
    }

    console.log("data", data);
  });

  // Create subscribe request based on provided arguments.
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
  if (args.accounts) {
    const filters: SubscribeRequestFilterAccountsFilter[] = [];

    if (args.accounts.memcmp) {
      for (let filter in args.accounts.memcmp) {
        const filterSpec = filter.split(",", 1);
        if (filterSpec.length != 2) {
          throw new Error("invalid memcmp");
        }

        const [offset, data] = filterSpec;
        filters.push({
          memcmp: { offset, base58: data.trim() },
        });
      }
    }

    if (args.accounts.tokenaccountstate) {
      filters.push({
        tokenAccountState: args.accounts.tokenaccountstate,
      });
    }

    if (args.accounts.datasize) {
      filters.push({ datasize: args.accounts.datasize });
    }

    if (args.accounts.lamports) {
      for (let filter in args.accounts.lamports) {
        const filterSpec = filter.split(":", 1);
        if (filterSpec.length != 2) {
          throw new Error("invalid lamports");
        }

        const [cmp, value] = filterSpec;
        let lamports: SubscribeRequestFilterAccountsFilterLamports = {};
        switch (cmp) {
          case "eq": {
            lamports.eq = value;
          }
          case "ne": {
            lamports.ne = value;
          }
          case "lt": {
            lamports.lt = value;
          }
          case "gt": {
            lamports.gt = value;
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
      account: args.accountsAccount,
      owner: args.accountsOwner,
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
      accountInclude: args.blocksAccountInclude,
      includeTransactions: args.blocksIncludeTransactions,
      includeAccounts: args.blocksIncludeAccounts,
      includeEntries: args.blocksIncludeEntries,
    };
  }

  if (args.blocksMeta) {
    request.blocksMeta.client = {
      account_include: args.blocksAccountInclude,
    };
  }

  if (args.accounts.dataslice) {
    for (let filter in args.accounts.dataslice) {
      const filterSpec = filter.split(",", 1);
      if (filterSpec.length != 2) {
        throw new Error("invalid data slice");
      }

      const [offset, length] = filterSpec;
      request.accountsDataSlice.push({
        offset,
        length,
      });
    }
  }

  if (args.ping) {
    request.ping = { id: args.ping };
  }

  // Send subscribe request
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

function parseCommandLineArgs() {
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
    })
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
      }
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
          type: "string",
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
    .command("fumarole-subscribe", "subscribe to events via fumarole", (yargs) => {
      return yargs.options({
        "consumer-group-label": {
          default: "",
          describe: "fumarole consumer group label",
          type: "string"
        },
        "subscription-id": {
          default: "",
          describe: "fumarole subscription id",
          type: "string"
        },
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
      });
    })
    .command("fumarole-create-consumer-group", "create a fumarole consumer group", (yargs) => {
      return yargs.options({
        "consumer-group-label": {
          default: "",
          describe: "fumarole consumer group label",
          type: "string"
        },
        "subscription-id": {
          default: "",
          describe: "fumarole subscription id",
          type: "string"
        },
      })
    })
    .command("fumarole-create-subscription-id", "create a fumarole subscription id", (yargs) => {
      return yargs.options({});
    })
    .command("fumarole-get-slot-lag-info", "get fumarole slot lag info", (yargs) => {
      return yargs.options({
        "consumer-group-label": {
          default: "",
          describe: "fumarole consumer group label",
          type: "string"
        },
        "subscription-id": {
          default: "",
          describe: "fumarole subscription id",
          type: "string"
        },
      })
    })
    .demandCommand(1)
    .help().argv;
}

main();