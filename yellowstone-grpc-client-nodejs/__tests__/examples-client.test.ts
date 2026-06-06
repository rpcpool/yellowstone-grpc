import { execFile, spawn } from "node:child_process";
import fs from "node:fs/promises";
import path from "node:path";

type CliResult = {
  stdout: string;
  stderr: string;
  code: number | null;
  signal: NodeJS.Signals | null;
};

type StreamCliResult = CliResult & {
  matchedOutput: string;
};

const TEST_TIMEOUT = 120_000;
const SUBSCRIPTION_TIMEOUT = 90_000;
const SUBSCRIPTION_TEST_TIMEOUT = SUBSCRIPTION_TIMEOUT + 10_000;
const PING_TIMEOUT = 30_000;

const NODEJS_DIR = process.cwd();
const REPO_ROOT = path.resolve(NODEJS_DIR, "..");
const EXAMPLE_DIR = path.join(REPO_ROOT, "examples", "typescript");
const EXAMPLE_CLI = path.join(EXAMPLE_DIR, "dist", "client.js");
const NODEJS_NODE_MODULES = path.join(NODEJS_DIR, "node_modules");
const EXAMPLE_NODE_MODULES = path.join(EXAMPLE_DIR, "node_modules");

const CLOCK_PUBKEY = "SysvarC1ock11111111111111111111111111111111";
const TOKEN_PROGRAM_PUBKEY = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA";
const ZERO_PUBKEY = "11111111111111111111111111111111";
const ZERO_SIGNATURE =
  "1111111111111111111111111111111111111111111111111111111111111111";

const endpoint = process.env.TEST_ENDPOINT;
const xToken = process.env.TEST_TOKEN;
const describeLive = endpoint ? describe : describe.skip;

function commandLine(args: string[]) {
  return `node ${path.relative(REPO_ROOT, EXAMPLE_CLI)} ${redactArgs(args).join(" ")}`;
}

function redactArgs(args: string[]) {
  const redacted = [...args];
  for (let index = 0; index < redacted.length; index += 1) {
    if (redacted[index] === "--x-token" && index + 1 < redacted.length) {
      redacted[index + 1] = "<redacted>";
      index += 1;
    }
  }

  return redacted;
}

function baseArgs() {
  if (!endpoint) {
    throw new Error("TEST_ENDPOINT is required");
  }

  return [
    "--endpoint",
    endpoint,
    ...(xToken ? ["--x-token", xToken] : []),
  ];
}

async function exists(filePath: string) {
  try {
    await fs.lstat(filePath);
    return true;
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") {
      return false;
    }
    throw error;
  }
}

async function ensureSymlink(
  targetPath: string,
  linkPath: string,
  type: "dir" | "file",
) {
  if (await exists(linkPath)) {
    return;
  }

  await fs.mkdir(path.dirname(linkPath), { recursive: true });
  await fs.symlink(targetPath, linkPath, type);
}

function nodeModulePath(packageName: string) {
  return path.join(NODEJS_NODE_MODULES, ...packageName.split("/"));
}

async function ensureExampleDependencies() {
  await ensureSymlink(
    NODEJS_DIR,
    path.join(EXAMPLE_NODE_MODULES, "@triton-one", "yellowstone-grpc"),
    "dir",
  );

  for (const packageName of ["typescript", "undici-types", "yargs"]) {
    await ensureSymlink(
      nodeModulePath(packageName),
      path.join(EXAMPLE_NODE_MODULES, packageName),
      "dir",
    );
  }

  for (const packageName of ["node", "yargs", "yargs-parser"]) {
    await ensureSymlink(
      nodeModulePath(`@types/${packageName}`),
      path.join(EXAMPLE_NODE_MODULES, "@types", packageName),
      "dir",
    );
  }

  await ensureSymlink(
    path.join(NODEJS_NODE_MODULES, "typescript", "bin", "tsc"),
    path.join(EXAMPLE_NODE_MODULES, ".bin", "tsc"),
    "file",
  );
}

function exampleEnv() {
  return {
    ...process.env,
    NODE_PATH: [
      EXAMPLE_NODE_MODULES,
      NODEJS_NODE_MODULES,
      process.env.NODE_PATH,
    ]
      .filter((value): value is string => Boolean(value))
      .join(path.delimiter),
  };
}

function execFilePromise(
  command: string,
  args: string[],
  options: { cwd: string; timeoutMs: number },
) {
  return new Promise<void>((resolve, reject) => {
    execFile(
      command,
      args,
      {
        cwd: options.cwd,
        timeout: options.timeoutMs,
        env: exampleEnv(),
      },
      (error, stdout, stderr) => {
        if (error) {
          reject(
            new Error(
              [
                `Command failed: ${command} ${args.join(" ")}`,
                `stdout:\n${stdout}`,
                `stderr:\n${stderr}`,
                `error:\n${error.message}`,
              ].join("\n\n"),
            ),
          );
          return;
        }
        resolve();
      },
    );
  });
}

function runCliToExit(args: string[], timeoutMs = TEST_TIMEOUT) {
  return new Promise<CliResult>((resolve, reject) => {
    const child = spawn(process.execPath, [EXAMPLE_CLI, ...args], {
      cwd: EXAMPLE_DIR,
      env: exampleEnv(),
    });

    let stdout = "";
    let stderr = "";
    const timeout = setTimeout(() => {
      child.kill("SIGTERM");
      reject(
        new Error(
          [
            `Timed out after ${timeoutMs}ms: ${commandLine(args)}`,
            `stdout:\n${stdout}`,
            `stderr:\n${stderr}`,
          ].join("\n\n"),
        ),
      );
    }, timeoutMs);

    child.stdout.setEncoding("utf8");
    child.stderr.setEncoding("utf8");
    child.stdout.on("data", (chunk) => {
      stdout += chunk;
    });
    child.stderr.on("data", (chunk) => {
      stderr += chunk;
    });
    child.on("error", (error) => {
      clearTimeout(timeout);
      reject(error);
    });
    child.on("close", (code, signal) => {
      clearTimeout(timeout);
      if (code !== 0) {
        reject(
          new Error(
            [
              `Command exited with ${code ?? signal}: ${commandLine(args)}`,
              `stdout:\n${stdout}`,
              `stderr:\n${stderr}`,
            ].join("\n\n"),
          ),
        );
        return;
      }
      resolve({ stdout, stderr, code, signal });
    });
  });
}

function runCliUntilOutput(
  args: string[],
  waitFor: (output: string) => boolean,
  timeoutMs = SUBSCRIPTION_TIMEOUT,
) {
  return new Promise<StreamCliResult>((resolve, reject) => {
    const child = spawn(process.execPath, [EXAMPLE_CLI, ...args], {
      cwd: EXAMPLE_DIR,
      env: exampleEnv(),
    });

    let stdout = "";
    let stderr = "";
    let matched = false;
    let closed = false;
    let timedOut = false;
    let timeout: NodeJS.Timeout | undefined;
    let killTimer: NodeJS.Timeout | undefined;

    const combinedOutput = () => `${stdout}\n${stderr}`;
    const cleanup = () => {
      if (timeout) {
        clearTimeout(timeout);
      }
      if (killTimer) {
        clearTimeout(killTimer);
      }
    };

    const stopAfterMatch = () => {
      if (matched) {
        return;
      }
      matched = true;
      child.kill("SIGTERM");
      killTimer = setTimeout(() => {
        if (!closed) {
          child.kill("SIGKILL");
        }
      }, 2_000);
    };

    const observe = () => {
      if (!matched && waitFor(combinedOutput())) {
        stopAfterMatch();
      }
    };

    timeout = setTimeout(() => {
      timedOut = true;
      child.kill("SIGTERM");
      killTimer = setTimeout(() => {
        if (!closed) {
          child.kill("SIGKILL");
        }
      }, 2_000);
    }, timeoutMs);

    child.stdout.setEncoding("utf8");
    child.stderr.setEncoding("utf8");
    child.stdout.on("data", (chunk) => {
      stdout += chunk;
      observe();
    });
    child.stderr.on("data", (chunk) => {
      stderr += chunk;
      observe();
    });
    child.on("error", (error) => {
      cleanup();
      reject(error);
    });
    child.on("close", (code, signal) => {
      closed = true;
      cleanup();

      if (matched) {
        resolve({
          stdout,
          stderr,
          code,
          signal,
          matchedOutput: combinedOutput(),
        });
        return;
      }

      reject(
        new Error(
          [
            `Command did not produce expected data before exit/timeout: ${commandLine(args)}`,
            `exit: ${
              timedOut
                ? `timeout after ${timeoutMs}ms (${code ?? signal ?? "no close status"})`
                : (code ?? signal ?? "closed")
            }`,
            `stdout:\n${stdout}`,
            `stderr:\n${stderr}`,
          ].join("\n\n"),
        ),
      );
    });
  });
}

function expectNoNativeOneofBridge(output: string) {
  expect(output).not.toContain("updateOneof");
}

function expectResponseShape(output: string, shape: RegExp) {
  expect(output).toContain("response:");
  expect(output).toMatch(shape);
  expectNoNativeOneofBridge(output);
}

function expectStreamShape(output: string, shape: RegExp) {
  expect(output).toContain("data");
  expect(output).toMatch(shape);
  expectNoNativeOneofBridge(output);
}

function hasPong(output: string) {
  return /data[\s\S]*pong:\s*{\s*id:\s*1\s*}/.test(output);
}

function subscribeArgs(...args: string[]) {
  return [...baseArgs(), "subscribe", ...args];
}

function subscribeDeshredArgs(...args: string[]) {
  return [...baseArgs(), "subscribeDeshred", ...args];
}

describeLive("examples/typescript/src/client.ts user-facing CLI contract", () => {
  beforeAll(async () => {
    await ensureExampleDependencies();
    await execFilePromise("npm", ["run", "build"], {
      cwd: EXAMPLE_DIR,
      timeoutMs: 60_000,
    });
  }, 70_000);

  describe("unary commands", () => {
    test(
      "subscribe-replay-info returns replay info shape",
      async () => {
        const result = await runCliToExit([
          ...baseArgs(),
          "subscribe-replay-info",
        ]);
        expectResponseShape(
          result.stdout,
          /firstAvailable:\s*(undefined|'[0-9]+')/,
        );
      },
      TEST_TIMEOUT,
    );

    test(
      "ping returns PongResponse shape",
      async () => {
        const result = await runCliToExit([...baseArgs(), "ping"]);
        expectResponseShape(result.stdout, /count:\s*1/);
      },
      TEST_TIMEOUT,
    );

    test(
      "get-version returns GetVersionResponse shape",
      async () => {
        const result = await runCliToExit([...baseArgs(), "get-version"]);
        expectResponseShape(result.stdout, /version:\s*'[^']+'/);
      },
      TEST_TIMEOUT,
    );

    test(
      "get-slot returns GetSlotResponse shape",
      async () => {
        const result = await runCliToExit([
          ...baseArgs(),
          "--commitment",
          "processed",
          "get-slot",
        ]);
        expectResponseShape(result.stdout, /slot:\s*'[0-9]+'/);
      },
      TEST_TIMEOUT,
    );

    test(
      "get-block-height returns GetBlockHeightResponse shape",
      async () => {
        const result = await runCliToExit([
          ...baseArgs(),
          "--commitment",
          "processed",
          "get-block-height",
        ]);
        expectResponseShape(result.stdout, /blockHeight:\s*'[0-9]+'/);
      },
      TEST_TIMEOUT,
    );

    test(
      "get-latest-blockhash returns GetLatestBlockhashResponse shape",
      async () => {
        const result = await runCliToExit([
          ...baseArgs(),
          "--commitment",
          "processed",
          "get-latest-blockhash",
        ]);
        expectResponseShape(
          result.stdout,
          /slot:\s*'[0-9]+'[\s\S]*blockhash:\s*'[^']+'[\s\S]*lastValidBlockHeight:\s*'[0-9]+'/,
        );
      },
      TEST_TIMEOUT,
    );

    test(
      "is-blockhash-valid returns IsBlockhashValidResponse shape",
      async () => {
        const latest = await runCliToExit([
          ...baseArgs(),
          "--commitment",
          "processed",
          "get-latest-blockhash",
        ]);
        const blockhash = latest.stdout.match(/blockhash:\s*'([^']+)'/)?.[1];
        expect(blockhash).toBeTruthy();

        const result = await runCliToExit([
          ...baseArgs(),
          "is-blockhash-valid",
          "--blockhash",
          blockhash!,
        ]);
        expectResponseShape(
          result.stdout,
          /slot:\s*'[0-9]+'[\s\S]*valid:\s*(true|false)/,
        );
      },
      TEST_TIMEOUT,
    );
  });

  describe("subscription commands return public SDK data shapes", () => {
    test(
      "subscribe --slots returns SubscribeUpdate.slot",
      async () => {
        const result = await runCliUntilOutput(
          subscribeArgs("--slots"),
          (output) => /data[\s\S]*slot:\s*{[\s\S]*slot:\s*'[0-9]+'/.test(output),
        );
        expectStreamShape(
          result.matchedOutput,
          /slot:\s*{[\s\S]*slot:\s*'[0-9]+'[\s\S]*status:/,
        );
      },
      SUBSCRIPTION_TEST_TIMEOUT,
    );

    test(
      "subscribe --accounts returns SubscribeUpdate.account",
      async () => {
        const result = await runCliUntilOutput(
          subscribeArgs("--accounts", "--accounts-owner", TOKEN_PROGRAM_PUBKEY),
          (output) =>
            /data[\s\S]*account:\s*{[\s\S]*account:\s*{[\s\S]*pubkey:\s*(?:Uint8Array|<Buffer)/.test(
              output,
            ),
        );
        expectStreamShape(
          result.matchedOutput,
          /account:\s*{[\s\S]*account:\s*{[\s\S]*pubkey:\s*(?:Uint8Array|<Buffer)[\s\S]*lamports:\s*'[0-9]+'/,
        );
      },
      SUBSCRIPTION_TEST_TIMEOUT,
    );

    test(
      "subscribe --transactions --transactions-parsed returns transaction data",
      async () => {
        const result = await runCliUntilOutput(
          subscribeArgs(
            "--transactions",
            "--transactions-vote=false",
            "--transactions-failed=false",
            "--transactions-account-include",
            TOKEN_PROGRAM_PUBKEY,
            "--transactions-parsed",
          ),
          (output) => /TX filters:[\s\S]*slot#[0-9]+/.test(output),
        );
        expect(result.matchedOutput).toMatch(/TX filters:[\s\S]*tx:/);
        expectNoNativeOneofBridge(result.matchedOutput);
      },
      SUBSCRIPTION_TEST_TIMEOUT,
    );

    test(
      "subscribe --blocks returns SubscribeUpdate.block",
      async () => {
        const result = await runCliUntilOutput(
          subscribeArgs(
            "--blocks",
            "--blocks-include-transactions",
            "--blocks-include-accounts",
            "--blocks-include-entries",
          ),
          (output) => /data[\s\S]*block:\s*{[\s\S]*blockhash:\s*'[^']+'/.test(output),
        );
        expectStreamShape(
          result.matchedOutput,
          /block:\s*{[\s\S]*slot:\s*'[0-9]+'[\s\S]*blockhash:\s*'[^']+'/,
        );
      },
      SUBSCRIPTION_TEST_TIMEOUT,
    );

    test(
      "subscribeDeshred returns SubscribeUpdateDeshred data",
      async () => {
        const result = await runCliUntilOutput(
          subscribeDeshredArgs("--deshred-vote=false"),
          (output) =>
            /data[\s\S]*deshredTransaction:\s*{[\s\S]*slot:\s*'[0-9]+'/.test(
              output,
            ),
        );
        expectStreamShape(
          result.matchedOutput,
          /deshredTransaction:\s*{[\s\S]*transaction:\s*{[\s\S]*signature:\s*(?:Uint8Array|<Buffer)/,
        );
      },
      SUBSCRIPTION_TEST_TIMEOUT,
    );
  });

  describe("subscribe filter flag combinations are accepted and produce data", () => {
    const cases: Array<{ name: string; args: string[] }> = [
      {
        name: "accounts explicit account",
        args: ["--accounts", "--accounts-account", CLOCK_PUBKEY, "--ping", "1"],
      },
      {
        name: "accounts compressed account filter",
        args: [
          "--accounts",
          "--accounts-compressed",
          "--accounts-compressed-capacity",
          "100",
          "--accounts-account",
          CLOCK_PUBKEY,
          "--ping",
          "1",
        ],
      },
      {
        name: "accounts owner filter",
        args: ["--accounts", "--accounts-owner", ZERO_PUBKEY, "--ping", "1"],
      },
      {
        name: "accounts memcmp filter",
        args: ["--accounts", "--accounts-memcmp", "0,1111", "--ping", "1"],
      },
      {
        name: "accounts datasize filter",
        args: ["--accounts", "--accounts-datasize", "165", "--ping", "1"],
      },
      {
        name: "accounts token account state filter",
        args: ["--accounts", "--accounts-tokenaccountstate", "--ping", "1"],
      },
      {
        name: "accounts lamports eq filter",
        args: ["--accounts", "--accounts-lamports", "eq:0", "--ping", "1"],
      },
      {
        name: "accounts lamports ne filter",
        args: ["--accounts", "--accounts-lamports", "ne:0", "--ping", "1"],
      },
      {
        name: "accounts lamports lt filter",
        args: ["--accounts", "--accounts-lamports", "lt:1000000", "--ping", "1"],
      },
      {
        name: "accounts lamports gt filter",
        args: ["--accounts", "--accounts-lamports", "gt:0", "--ping", "1"],
      },
      {
        name: "accounts nonempty transaction signature filter",
        args: ["--accounts", "--accounts-nonemptytxnsignature", "--ping", "1"],
      },
      {
        name: "accounts data slice filter",
        args: [
          "--accounts",
          "--accounts-account",
          CLOCK_PUBKEY,
          "--accounts-dataslice",
          "0,8",
          "--ping",
          "1",
        ],
      },
      {
        name: "slots filter by commitment",
        args: ["--slots", "--slots-filter-by-commitment", "--ping", "1"],
      },
      {
        name: "transactions account and status filters",
        args: [
          "--transactions",
          "--transactions-vote=false",
          "--transactions-failed=false",
          "--transactions-decode-err",
          "--transactions-signature",
          ZERO_SIGNATURE,
          "--transactions-account-include",
          TOKEN_PROGRAM_PUBKEY,
          "--transactions-account-exclude",
          ZERO_PUBKEY,
          "--transactions-account-required",
          TOKEN_PROGRAM_PUBKEY,
          "--ping",
          "1",
        ],
      },
      {
        name: "transactionsStatus account and status filters",
        args: [
          "--transactions-status",
          "--transactions-status-vote=false",
          "--transactions-status-failed=false",
          "--transactions-status-signature",
          ZERO_SIGNATURE,
          "--transactions-status-account-include",
          TOKEN_PROGRAM_PUBKEY,
          "--transactions-status-account-exclude",
          ZERO_PUBKEY,
          "--transactions-status-account-required",
          TOKEN_PROGRAM_PUBKEY,
          "--ping",
          "1",
        ],
      },
      {
        name: "entry filter",
        args: ["--entry", "--ping", "1"],
      },
      {
        name: "blocks include flags",
        args: [
          "--blocks",
          "--blocks-include-transactions",
          "--blocks-include-accounts",
          "--blocks-include-entries",
          "--ping",
          "1",
        ],
      },
      {
        name: "blocks compressed account include filter",
        args: [
          "--blocks",
          "--blocks-compressed",
          "--blocks-compressed-capacity",
          "100",
          "--blocks-account-include",
          TOKEN_PROGRAM_PUBKEY,
          "--ping",
          "1",
        ],
      },
      {
        name: "blocksMeta filter",
        args: ["--blocks-meta", "--ping", "1"],
      },
      {
        name: "autoreconnect initial subscribe request",
        args: [
          "--autoreconnect",
          "--autoreconnect-initial-interval-ms",
          "10",
          "--autoreconnect-multiplier",
          "1",
          "--autoreconnect-max-retries",
          "2",
          "--slots",
          "--ping",
          "1",
        ],
      },
    ];

    test.each(cases)(
      "$name",
      async ({ args }) => {
        const result = await runCliUntilOutput(
          subscribeArgs(...args),
          hasPong,
          PING_TIMEOUT,
        );
        expectStreamShape(result.matchedOutput, /pong:\s*{\s*id:\s*1\s*}/);
      },
      PING_TIMEOUT + 5_000,
    );
  });

  describe("subscribeDeshred filter flag combinations are accepted and produce data", () => {
    const cases: Array<{ name: string; args: string[] }> = [
      {
        name: "vote and account filters",
        args: [
          "--deshred-vote=false",
          "--deshred-account-include",
          TOKEN_PROGRAM_PUBKEY,
          "--deshred-account-exclude",
          ZERO_PUBKEY,
          "--deshred-account-required",
          TOKEN_PROGRAM_PUBKEY,
          "--ping",
          "1",
        ],
      },
      {
        name: "parsed flag",
        args: ["--deshred-parsed", "--deshred-vote=false", "--ping", "1"],
      },
    ];

    test.each(cases)(
      "$name",
      async ({ args }) => {
        const result = await runCliUntilOutput(
          subscribeDeshredArgs(...args),
          hasPong,
          PING_TIMEOUT,
        );
        expectStreamShape(result.matchedOutput, /pong:\s*{\s*id:\s*1\s*}/);
      },
      PING_TIMEOUT + 5_000,
    );
  });
});
