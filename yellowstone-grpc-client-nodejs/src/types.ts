import { type GetTransactionApi } from "@solana/rpc-api";
import { type Signature } from "@solana/keys/dist/types";

const fakeGetTransition: GetTransactionApi["getTransaction"] = (
  signature,
  config
) => null;

const signature = "" as Signature;

const base58 = fakeGetTransition(signature, { encoding: "base58" });
const base64 = fakeGetTransition(signature, { encoding: "base64" });
const json = fakeGetTransition(signature, { encoding: "json" });
const jsonParsed = fakeGetTransition(signature, { encoding: "jsonParsed" });

export type MapTransactionEncodingToReturnType = {
  0: typeof base58; // legacy (binary)
  1: typeof base64;
  2: typeof base58;
  3: typeof json;
  4: typeof jsonParsed;
};
