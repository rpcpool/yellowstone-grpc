// This script measures the throughput of a given gRPC endpoint
// It:
// 1. subscribes to all data for all filters for given duration (default: 30s)
// 2. log the data to a txt file
// 3. measure and the size of the txt file
// Throughput = data that you care about subscribed to, received and processed
// Usage:
// tsx src/throughput-measurement.ts --endpoint <YOUR ENDPOINT> --x-token <OPTIONAL X TOKEN> --duration <OPTIONAL TEST DURATION (default: 30s)>

import yargs from "yargs";
import fs from "node:fs";
import path from "node:path";
import Client, { SubscribeRequest } from "@triton-one/yellowstone-grpc";

async function main() {
  let client: Client | undefined;
  let stream: any;
  let writeStream: fs.WriteStream | undefined;
  const args = parseCommandLineArgs();
  
  // Create measurement-outputs directory if it doesn't exist
  const outputDir = "measurement-outputs";
  if (!fs.existsSync(outputDir)) {
    fs.mkdirSync(outputDir, { recursive: true });
    console.log(`Created directory: ${outputDir}`);
  }
  
  const outputFilename = path.join(outputDir, `${Date.now()}.txt`);

  try {
    // Create output file stream
    writeStream = fs.createWriteStream(outputFilename);
    console.log(`Writing data to: ${outputFilename}`);

    // Initialize gRPC client
    console.log(`Connecting to endpoint: ${args.endpoint}`);
    client = new Client(args.endpoint, args.xToken, {
      grpcMaxDecodingMessageSize: 64 * 1024 * 1024, // 64MiB
    });

    await client.connect();
    console.log("Connected successfully");

    // Subscribe for events
    stream = await client.subscribe();

    // Create promises for stream handling
    const streamClosed = new Promise<void>((resolve, reject) => {
      stream.on("error", (error) => {
        console.error("Stream error:", error);
        reject(error);
      });
      stream.on("end", () => {
        console.log("Stream ended");
        resolve();
      });
      stream.on("close", () => {
        console.log("Stream closed");
        resolve();
      });
    });

    // Handle incoming data
    stream.on("data", (data) => {
      if (writeStream && !writeStream.destroyed) {
        writeStream.write(JSON.stringify(data, null, 2) + "\n");
      }
    });

    // Create subscribe request for everything to get loads of data
    const request: SubscribeRequest = {
      accounts: {
        allAccounts: {
          account: [],
          filters: [],
          owner: []
        },
      },
      slots: {
        allSlots: {},
      },
      transactions: {
        allTransactions: {
          accountExclude: [],
          accountInclude: [],
          accountRequired: []
        }
      },
      transactionsStatus: {
        allTransactionsStatus: {
          accountExclude: [],
          accountInclude: [],
          accountRequired: []
        }
      },
      entry: {
        allEntries: {},
      },
      blocks: {
        allBlocks: {
          accountInclude: [],
        }
      },
      blocksMeta: {
        allBlocksMeta: {}
      },
      accountsDataSlice: [],
      ping: undefined,
    };

    // Send subscribe request
    await new Promise<void>((resolve, reject) => {
      stream.write(request, (err) => {
        if (err === null || err === undefined) {
          resolve();
        } else {
          reject(err);
        }
      });
    });

    console.log(`Collecting data for ${args.duration} seconds...`);

    // Set up duration timer
    const timerPromise = new Promise<void>((resolve) => {
      setTimeout(() => {
        console.log("Duration elapsed, stopping subscription...");
        stream.end();
        resolve();
      }, args.duration * 1000);
    });

    // Wait for either timer to complete or stream to close
    await Promise.race([timerPromise, streamClosed]);

    // Close write stream and wait for it to finish
    if (writeStream && !writeStream.destroyed) {
      await new Promise<void>((resolve, reject) => {
        writeStream!.end(() => {
          resolve();
        });
        writeStream!.on("error", reject);
      });
    }

    // Wait briefly to ensure all writes are flushed
    await new Promise((resolve) => setTimeout(resolve, 100));

    // Measure file size
    const stats = fs.statSync(outputFilename);
    const fileSizeInBytes = stats.size;
    const fileSizeInMB = fileSizeInBytes / (1024 * 1024);

    // Log results
    console.log("\n=== Throughput Measurement Results ===");
    console.log(`File: ${outputFilename}`);
    console.log(`Size: ${fileSizeInMB.toFixed(2)} MB`);
    console.log(`Duration: ${args.duration} seconds`);
    console.log(`Throughput: ${(fileSizeInMB / args.duration).toFixed(2)} MB/s`);
    console.log("=====================================\n");
    process.exit(1)
  } catch (error) {
    console.error("Error:", error);
    process.exit(1);
  } finally {
    await cleanup(client, stream, writeStream);
  }
}

async function cleanup(
  client: Client | undefined,
  stream: any,
  writeStream: fs.WriteStream | undefined
) {
  try {
    if (stream && !stream.destroyed) {
      stream.end();
    }
    if (writeStream && !writeStream.destroyed) {
      writeStream.end();
    }
    // Wait briefly for cleanup to complete
    await new Promise((resolve) => setTimeout(resolve, 100));
  } catch (error) {
    console.error("Cleanup error:", error);
  }
}

function parseCommandLineArgs() {
  return yargs(process.argv.slice(2))
    .options({
      endpoint: {
        alias: "e",
        describe: "gRPC endpoint",
        type: "string",
        demandOption: true,
      },
      "x-token": {
        describe: "token for auth, can be used only with ssl",
        type: "string",
      },
      duration: {
        alias: "d",
        describe: "duration in seconds to collect data",
        type: "number",
        default: 30,
      },
    })
    .help().argv;
}

main();

