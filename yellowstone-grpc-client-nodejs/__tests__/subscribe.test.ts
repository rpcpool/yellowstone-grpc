import Client, {
  CommitmentLevel,
  CompressedAccountFilterSet,
  TokenAccountExpansionControlFlag,
} from "../src";
import type { SubscribeRequest, SubscribeUpdate } from "../src";

const ACTIVE_ACCOUNT_PUBKEY = "ping6gwBZx1ccMMFyLgkVSupUmujYrFidEXuNRPq989";
const SUBSCRIBE_TIMEOUT_MS = Number(
  process.env.SUBSCRIBE_ALL_UPDATES_TIMEOUT_MS ?? 60_000,
);

const endpoint = process.env.TEST_ENDPOINT;
const xToken = process.env.TEST_TOKEN;
const describeLive = endpoint ? describe : describe.skip;

// Block footers only exist on Alpenglow clusters, so they are checked against a separate
// endpoint. TEST_ENDPOINT may point at a cluster without Alpenglow, where no footer ever
// arrives.
const alpenglowEndpoint = process.env.TEST_ALPENGLOW_ENDPOINT;
const alpenglowToken = process.env.TEST_ALPENGLOW_TOKEN;
const describeAlpenglow = alpenglowEndpoint ? describe : describe.skip;

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

      const compressedAccounts = new CompressedAccountFilterSet(1);
      compressedAccounts.insert(ACTIVE_ACCOUNT_PUBKEY);

      const request: SubscribeRequest = {
        accounts: {
          accountsClient: {
            account: [ACTIVE_ACCOUNT_PUBKEY],
            owner: [],
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
            accountInclude: [ACTIVE_ACCOUNT_PUBKEY],
            accountExclude: [],
            accountRequired: [],
            tokenAccounts: TokenAccountExpansionControlFlag.ALL,
          },
          compressedTransactionsClient: {
            ...compressedAccounts.toTransactionFilter(),
            tokenAccounts: TokenAccountExpansionControlFlag.BALANCE_CHANGED,
          },
        },
        transactionsStatus: {
          transactionsStatusClient: {
            vote: false,
            failed: false,
            accountInclude: [ACTIVE_ACCOUNT_PUBKEY],
            accountExclude: [],
            accountRequired: [],
            tokenAccounts: TokenAccountExpansionControlFlag.ALL,
          },
          compressedTransactionsStatusClient: {
            ...compressedAccounts.toTransactionFilter(),
            tokenAccounts: TokenAccountExpansionControlFlag.BALANCE_CHANGED,
          },
        },
        blocks: {
          blocksClient: {
            accountInclude: [ACTIVE_ACCOUNT_PUBKEY],
            includeTransactions: true,
            includeAccounts: true,
            includeEntries: true,
          },
        },
        blocksMeta: { blocksMetaClient: {} },
        blockFooter: { blockFooterClient: {} },
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
        blockFooter: {},
        entry: {},
        accountsDataSlice: [],
        ping: { id: 1 },
      };

      const seen = {
        account: false,
        slot: false,
        transaction: false,
        compressedTransaction: false,
        transactionStatus: false,
        compressedTransactionStatus: false,
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
            seen.transaction ||=
              update.transaction !== undefined &&
              update.filters.includes("transactionsClient");
            seen.compressedTransaction ||=
              update.transaction !== undefined &&
              update.filters.includes("compressedTransactionsClient");
            seen.transactionStatus ||=
              update.transactionStatus !== undefined &&
              update.filters.includes("transactionsStatusClient");
            seen.compressedTransactionStatus ||=
              update.transactionStatus !== undefined &&
              update.filters.includes("compressedTransactionsStatusClient");
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

describeAlpenglow("Client.subscribe block footers", () => {
  test(
    "receives a block footer with its finalization certificate",
    async () => {
      if (!alpenglowEndpoint) {
        throw new Error(
          "TEST_ALPENGLOW_ENDPOINT is required for block footer tests",
        );
      }

      const client = new Client(
        alpenglowEndpoint,
        alpenglowToken,
        undefined,
        undefined,
      );
      await client.connect();

      const request: SubscribeRequest = {
        accounts: {},
        slots: {},
        transactions: {},
        transactionsStatus: {},
        blocks: {},
        blocksMeta: {},
        blockFooter: { blockFooterClient: { includeCertificates: true } },
        entry: {},
        commitment: CommitmentLevel.PROCESSED,
        accountsDataSlice: [],
        ping: undefined,
      };

      const stream = await client.subscribe(request);

      try {
        const footer = await new Promise<
          NonNullable<SubscribeUpdate["blockFooter"]>
        >((resolve, reject) => {
          const timeout = setTimeout(
            () =>
              reject(
                new Error(
                  `Timed out after ${SUBSCRIBE_TIMEOUT_MS}ms waiting for a block footer`,
                ),
              ),
            SUBSCRIBE_TIMEOUT_MS,
          );
          stream.on("data", (update: SubscribeUpdate) => {
            if (update.blockFooter !== undefined) {
              clearTimeout(timeout);
              resolve(update.blockFooter);
            }
          });
          stream.on("error", (error: Error) => {
            clearTimeout(timeout);
            reject(error);
          });
        });

        expect(footer.bankHash.length).toBe(32);
        expect(footer.blockFinalCert?.length ?? 0).toBeGreaterThan(0);
      } finally {
        stream.destroy();
      }
    },
    SUBSCRIBE_TIMEOUT_MS + 10_000,
  );
});
