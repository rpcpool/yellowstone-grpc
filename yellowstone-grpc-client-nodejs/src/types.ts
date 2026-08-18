import type {
  GetTransactionApiResponseBase58,
  GetTransactionApiResponseBase64,
  GetTransactionApiResponseJson,
  GetTransactionApiResponseJsonParsed,
} from "@solana/rpc-api";
import { type TransactionError } from "@solana/rpc-types";

export type MapTransactionEncodingToReturnType = {
  0: GetTransactionApiResponseBase58 | null; // legacy (binary)
  1: GetTransactionApiResponseBase64 | null;
  2: GetTransactionApiResponseBase58 | null;
  3: GetTransactionApiResponseJson | null;
  4: GetTransactionApiResponseJsonParsed | null;
};

export type DeshredTransactionEncodingToReturnType<
  T extends keyof MapTransactionEncodingToReturnType = keyof MapTransactionEncodingToReturnType,
> = {
  signature: string;
  isVote: boolean;
  transaction: MapTransactionEncodingToReturnType[T];
  loadedWritableAddresses: string[];
  loadedReadonlyAddresses: string[];
};

export type TransactionErrorSolana = TransactionError;
