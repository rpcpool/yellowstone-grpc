// Import generated gRPC client and types.
import { GeyserClient, SubscribeRequest } from "./grpc/geyser";

import { ChannelCredentials, credentials, Metadata } from "@grpc/grpc-js";
import yargs from 'yargs';

async function main() {
  const args = yargs(process.argv.slice(2))
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

  const endpointURL = new URL(args.endpoint);

  // Open connection
  let creds: ChannelCredentials;
  if (endpointURL.protocol === "https:") {
    creds = credentials.combineChannelCredentials(
      credentials.createSsl(),
      credentials.createFromMetadataGenerator((_params, callback) => {
        const metadata = new Metadata();
        if (args.xToken !== undefined) {
          metadata.add(`x-token`, args.xToken);
        }
        return callback(null, metadata);
      })
    );
  } else {
    creds = ChannelCredentials.createInsecure();
  }

  // Create the client
  const client = new GeyserClient(endpointURL.host, creds);
  const stream = await client.subscribe();

  // Create `error` / `end` handler
  const stream_closed = new Promise<void>((resolve, reject) => {
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
  const request: SubscribeRequest = {
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
      vote: args.transactionsVote,
      failed: args.transactionsFailed,
      signature: args.transactionsSignature,
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
  await new Promise<void>((resolve, reject) => {
    stream.write(request, (err) => {
      stream.end();
      if (err === null) {
        resolve();
      } else {
        reject(err);
      }
    });
  });

  await stream_closed;
}

main();
