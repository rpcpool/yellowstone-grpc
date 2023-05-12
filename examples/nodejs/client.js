const protoLoader = require("@grpc/proto-loader");
const grpc = require("@grpc/grpc-js");

async function main() {
  const args = require("yargs")(process.argv.slice(2))
    .options({
      endpoint: {
        default: "http://localhost:10000",
        describe: "gRPC endpoint",
        type: "string",
      },
      "x-token": {
        describe: "token for auth, can be used only with ssl",
        type: "string",
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
      "accounts-owner": {
        default: [],
        describe: "filter by owner pubkey",
        type: "array",
      },
      slots: {
        default: false,
        describe: "subscribe on slots updates",
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
      blocks: {
        default: false,
        description: "subscribe on block updates",
        type: "boolean",
      },
      "blocks-meta": {
        default: false,
        description: "subscribe on block meta updates (without transactions)",
        type: "boolean",
      },
    })
    .help().argv;

  // Load proto
  const defs = await protoLoader.load("../../yellowstone-grpc-proto/proto/geyser.proto");
  const pkg = grpc.loadPackageDefinition(defs);

  // Open connection
  const endpointURL = new URL(args.endpoint);

  let creds;
  if (endpointURL.protocol === "https:") {
    creds = grpc.credentials.combineChannelCredentials(
      grpc.credentials.createSsl(),
      grpc.credentials.createFromMetadataGenerator((_params, callback) => {
        const metadata = new grpc.Metadata();
        if (args.xToken !== undefined) {
          metadata.add(`x-token`, args.xToken);
        }
        return callback(null, metadata);
      })
    );
  } else {
    creds = grpc.credentials.createInsecure();
  }
  const client = new pkg.geyser.Geyser(endpointURL.host, creds);
  const stream = await client.subscribe();

  // Create `error` / `end` handler
  const stream_closed = new Promise((resolve, reject) => {
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
    console.log("data", data);
  });

  // Create subscribe request
  const request = {
    accounts: {},
    slots: {},
    transactions: {},
    blocks: {},
    blocksMeta: {},
  };
  if (args.accounts) {
    request.accounts.client = {
      account: args.accountsAccount,
      owner: args.accountsOwner,
    };
  }
  if (args.slots) {
    request.slots.client = {};
  }
  if (args.transactions) {
    request.transactions.client = {
      vote: args.transactionsVote === undefined ? null : args.transactionsVote,
      failed:
        args.transactionsFailed === undefined ? null : args.transactionsFailed,
      signature:
        args.transactionsSignature === undefined
          ? null
          : args.transactionsSignature,
      accountInclude: args.transactionsAccountInclude,
      accountExclude: args.transactionsAccountExclude,
    };
  }
  if (args.blocks) {
    request.blocks.client = {};
  }
  if (args.blocksMeta) {
    request.blocksMeta.client = {};
  }

  // Send subscribe request
  await new Promise((resolve, reject) => {
    stream.write(request, (err) => {
      stream.end();
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

  await stream_closed;
}

main();
