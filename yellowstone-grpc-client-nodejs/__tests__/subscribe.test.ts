import Client, { CommitmentLevel } from "../src";
import type { SubscribeRequest, SubscribeUpdate } from "../src";

const TOKEN_PROGRAM_PUBKEY = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA";
const SUBSCRIBE_TIMEOUT_MS = Number(
  process.env.SUBSCRIBE_ALL_UPDATES_TIMEOUT_MS ?? 60_000,
);

const endpoint = process.env.TEST_ENDPOINT;
const xToken = process.env.TEST_TOKEN;
const describeLive = endpoint ? describe : describe.skip;

describeLive("Client.subscribe", () => {
  test(
    "subscribes to every SubscribeRequest filter and receives every SubscribeUpdate type",
    async () => {
      if (!endpoint) {
        throw new Error("TEST_ENDPOINT is required for live subscribe tests");
      }

      const client = new Client(
        endpoint,
        xToken,
        { grpcMaxDecodingMessageSize: 64 * 1024 * 1024 },
        undefined,
      );
      await client.connect();

      const request: SubscribeRequest = {
        accounts: {
          accountsClient: {
            account: [],
            owner: [TOKEN_PROGRAM_PUBKEY],
            filters: [],
          },
        },
        slots: {
          slotsClient: {
            filterByCommitment: false,
            interslotUpdates: true,
          },
        },
        transactions: {
          transactionsClient: {
            vote: false,
            failed: false,
            accountInclude: [TOKEN_PROGRAM_PUBKEY],
            accountExclude: [],
            accountRequired: [],
          },
        },
        transactionsStatus: {
          transactionsStatusClient: {
            vote: false,
            failed: false,
            accountInclude: [TOKEN_PROGRAM_PUBKEY],
            accountExclude: [],
            accountRequired: [],
          },
        },
        blocks: {
          blocksClient: {
            accountInclude: [TOKEN_PROGRAM_PUBKEY],
            includeTransactions: true,
            includeAccounts: true,
            includeEntries: true,
          },
        },
        blocksMeta: { blocksMetaClient: {} },
        entry: { entryClient: {} },
        commitment: CommitmentLevel.PROCESSED,
        accountsDataSlice: [{ offset: "0", length: "1" }],
        ping: undefined,
      };

      const pingRequest: SubscribeRequest = {
        accounts: {},
        slots: {},
        transactions: {},
        transactionsStatus: {},
        blocks: {},
        blocksMeta: {},
        entry: {},
        accountsDataSlice: [],
        ping: { id: 1 },
      };

      const seen = {
        account: false,
        slot: false,
        transaction: false,
        transactionStatus: false,
        block: false,
        ping: false,
        pong: false,
        blockMeta: false,
        entry: false,
      };

      const stream = await client.subscribe(request);

      try {
        await new Promise<void>((resolve, reject) => {
          let settled = false;

          const cleanup = () => {
            clearTimeout(timeout);
            stream.off("data", onData);
            stream.off("error", onError);
            stream.off("end", onEndOrClose);
            stream.off("close", onEndOrClose);
          };

          const finish = (fn: () => void) => {
            if (settled) {
              return;
            }
            settled = true;
            cleanup();
            fn();
          };

          const missingTypes = () =>
            Object.entries(seen)
              .filter(([, value]) => !value)
              .map(([name]) => name);

          const onData = (update: SubscribeUpdate) => {
            seen.account ||= update.account !== undefined;
            seen.slot ||= update.slot !== undefined;
            seen.transaction ||= update.transaction !== undefined;
            seen.transactionStatus ||= update.transactionStatus !== undefined;
            seen.block ||= update.block !== undefined;
            seen.ping ||= update.ping !== undefined;
            seen.pong ||= update.pong !== undefined;
            seen.blockMeta ||= update.blockMeta !== undefined;
            seen.entry ||= update.entry !== undefined;

            if (missingTypes().length === 0) {
              finish(resolve);
            }
          };

          const onError = (error: Error) => finish(() => reject(error));
          const onEndOrClose = () =>
            finish(() =>
              reject(
                new Error(
                  `Subscribe stream ended before all update types were seen; missing: ${missingTypes().join(", ")}`,
                ),
              ),
            );

          const timeout = setTimeout(() => {
            finish(() =>
              reject(
                new Error(
                  `Timed out after ${SUBSCRIBE_TIMEOUT_MS}ms waiting for all update types; missing: ${missingTypes().join(", ")}`,
                ),
              ),
            );
          }, SUBSCRIBE_TIMEOUT_MS);

          stream.on("data", onData);
          stream.on("error", onError);
          stream.on("end", onEndOrClose);
          stream.on("close", onEndOrClose);

          stream.write(pingRequest, (error) => {
            if (error) {
              finish(() => reject(error));
            }
          });
        });
      } finally {
        stream.destroy();
      }
    },
    SUBSCRIBE_TIMEOUT_MS + 10_000,
  );
});
