/** TypeScript/JavaScript client for gRPC Geyser. */

// Import generated gRPC client and types.
import {
  GetBlockHeightResponse as GetBlockHeightResponseMessage,
  GetLatestBlockhashResponse as GetLatestBlockhashResponseMessage,
  GetSlotResponse as GetSlotResponseMessage,
  GetVersionResponse as GetVersionResponseMessage,
  IsBlockhashValidResponse as IsBlockhashValidResponseMessage,
  PongResponse as PongResponseMessage,
  SubscribeReplayInfoResponse as SubscribeReplayInfoResponseMessage,
  SubscribeUpdateTransactionInfo,
} from "./grpc/geyser";
import type {
  CommitmentLevel,
  GetBlockHeightResponse,
  GetLatestBlockhashResponse,
  GetSlotResponse,
  GetVersionResponse,
  IsBlockhashValidResponse,
  PongResponse,
  SubscribeReplayInfoResponse,
  SubscribeRequest,
  SubscribeUpdate,
} from "./grpc/geyser";

// Reexport automatically generated types
export {
  CommitmentLevel,
  SubscribeRequest,
  SubscribeRequest_AccountsEntry,
  SubscribeRequest_BlocksEntry,
  SubscribeRequest_BlocksMetaEntry,
  SubscribeRequest_SlotsEntry,
  SubscribeRequest_TransactionsEntry,
  SubscribeRequestAccountsDataSlice,
  SubscribeRequestFilterAccounts,
  SubscribeRequestFilterAccountsFilter,
  SubscribeRequestFilterAccountsFilterLamports,
  SubscribeRequestFilterAccountsFilterMemcmp,
  SubscribeRequestFilterBlocks,
  SubscribeRequestFilterBlocksMeta,
  SubscribeRequestFilterEntry,
  SubscribeRequestFilterSlots,
  SubscribeRequestFilterTransactions,
  SubscribeUpdate,
  SubscribeUpdateAccount,
  SubscribeUpdateAccountInfo,
  SubscribeUpdateBlock,
  SubscribeUpdateBlockMeta,
  SubscribeUpdatePing,
  SubscribeUpdateSlot,
  SubscribeUpdateTransaction,
  SubscribeUpdateTransactionInfo,
} from "./grpc/geyser";

import type {
  TransactionErrorSolana,
  // Import mapper to get return type based on WasmUiTransactionEncoding
  MapTransactionEncodingToReturnType,
} from "./types";

import { Duplex } from "stream";
import * as napi from "./napi/index";

/**
 * Public channel options accepted by the SDK constructor.
 * This is sourced from the native N-API constructor signature to avoid
 * duplicating option fields in this file.
 */
export type ChannelOptions = NonNullable<Parameters<typeof napi.GrpcClient.new>[2]>;

/**
 * Convert N-API `JsSubscribeUpdate` shape (with `updateOneof`) into the
 * generated protobuf-friendly SDK shape (`SubscribeUpdate`) where the oneof
 * variants are top-level optional fields.
 */
function fromJsSubscribeUpdate(update: napi.JsSubscribeUpdate): SubscribeUpdate {
  const oneof = update.updateOneof ?? {};

  return {
    filters: update.filters ?? [],
    createdAt: update.createdAt,
    account: oneof.account as any,
    slot: oneof.slot as any,
    transaction: oneof.transaction as any,
    transactionStatus: oneof.transactionStatus as any,
    block: oneof.block as any,
    ping: oneof.ping as any,
    pong: oneof.pong as any,
    blockMeta: oneof.blockMeta as any,
    entry: oneof.entry as any,
  } as SubscribeUpdate;
}

export default class Client {
  private _insecureEndpoint: string;
  private _insecureXToken: string | undefined;
  private _channelOptions: ChannelOptions | undefined;
  private _grpcClient: unknown | null = null;

  constructor(
    endpoint: string,
    xToken: string | undefined,
    channel_options: ChannelOptions | undefined,
  ) {
    this._insecureEndpoint = endpoint;
    this._insecureXToken = xToken;
    this._channelOptions = channel_options;
  }

  private _connectedGrpcClient(): napi.GrpcClient {
    if (!this._grpcClient) {
      throw new Error("Client not connected. Call connect() first");
    }
    return this._grpcClient as napi.GrpcClient;
  }

  async connect(): Promise<void> {
    // Establish one persistent native gRPC client reused by all calls.
    this._grpcClient = await napi.GrpcClient.new(
      this._insecureEndpoint,
      this._insecureXToken,
      this._channelOptions,
    );
  }

  async getLatestBlockhash(
    commitment?: CommitmentLevel,
  ): Promise<GetLatestBlockhashResponse> {
    const grpcClient = this._connectedGrpcClient();

    const request: napi.JsGetLatestBlockhashRequest = {
      commitment: commitment ?? null,
    };

    const response = await grpcClient.getLatestBlockhash(request);
    return GetLatestBlockhashResponseMessage.fromPartial({
      slot: response.slot,
      blockhash: response.blockhash,
      lastValidBlockHeight: response.lastValidBlockHeight,
    });
  }

  async ping(count: number): Promise<PongResponse> {
    const grpcClient = this._connectedGrpcClient();

    const request: napi.JsPingRequest = {
      count,
    };

    const response = await grpcClient.ping(request);
    return PongResponseMessage.fromPartial({
      count: response.count,
    });
  }

  async getBlockHeight(commitment?: CommitmentLevel): Promise<GetBlockHeightResponse> {
    const grpcClient = this._connectedGrpcClient();

    const request: napi.JsGetBlockHeightRequest = {
      commitment,
    };

    const response = await grpcClient.getBlockHeight(request);
    return GetBlockHeightResponseMessage.fromPartial({
      blockHeight: response.blockHeight,
    });
  }

  async getSlot(commitment?: CommitmentLevel): Promise<GetSlotResponse> {
    const grpcClient = this._connectedGrpcClient();

    const request: napi.JsGetSlotRequest = {
      commitment,
    };

    const response = await grpcClient.getSlot(request);
    return GetSlotResponseMessage.fromPartial({
      slot: response.slot,
    });
  }

  async isBlockhashValid(
    blockhash: string,
    commitment?: CommitmentLevel,
  ): Promise<IsBlockhashValidResponse> {
    const grpcClient = this._connectedGrpcClient();

    const request: napi.JsIsBlockhashValidRequest = {
      blockhash,
      commitment,
    };

    const response = await grpcClient.isBlockhashValid(request);
    return IsBlockhashValidResponseMessage.fromPartial({
      slot: response.slot,
      valid: response.valid,
    });
  }

  async getVersion(): Promise<GetVersionResponse> {
    const grpcClient = this._connectedGrpcClient();

    const request: napi.JsGetVersionRequest = {};

    const response = await grpcClient.getVersion(request);
    return GetVersionResponseMessage.fromPartial({
      version: response.version,
    });
  }

  async subscribeReplayInfo(): Promise<SubscribeReplayInfoResponse> {
    const grpcClient = this._connectedGrpcClient();

    const request: napi.JsSubscribeReplayInfoRequest = {};

    const response = await grpcClient.subscribeReplayInfo(request);
    return SubscribeReplayInfoResponseMessage.fromPartial({
      firstAvailable: response.firstAvailable,
    });
  }

  async subscribe(): Promise<ClientDuplexStream> {
    const grpcClient = this._connectedGrpcClient();

    // Inner stream.Duplex config passed to both stream.Readable and Writable.
    // See: https://nodejs.org/en/blog/feature/streams2#new-streamduplexoptions
    const options = {
      // Pass objects not bytes.
      objectMode: true,
      // Skip unnecessary buffer conversion for performance.
      decodeStrings: false,
      // TODO: Fine tune high watermark for performance and backpressure.
      // highWaterMark: 16
    };

    // Native stream produces N-API generated JS objects; wrapper below adapts
    // to public protobuf-generated SDK shapes and Node stream semantics.
    const stream = grpcClient.subscribe();

    return new Promise<ClientDuplexStream>((resolve, reject) => {
      try {
        resolve(new ClientDuplexStream(stream, options));
      } catch (err) {
        reject(err);
      }
    });
  }
}

export class ClientDuplexStream extends Duplex {
  private _napiDuplexStream: unknown;
  // Prevent overlapping native reads: a single pending read at a time.
  private _readInFlight: boolean;
  // Closed once Node emits `close`.
  private _isClosed: boolean;
  // Set during destroy path to short-circuit late async completions.
  private _isDestroying: boolean;
  // Ensure we surface at most one terminal error per stream instance.
  private _terminalErrorSeen: boolean;

  constructor(stream: unknown, options: object | undefined) {
    super({ ...options });
    this._napiDuplexStream = stream;
    this._readInFlight = false;
    this._isClosed = false;
    this._isDestroying = false;
    this._terminalErrorSeen = false;

    this.once("close", () => {
      this._isClosed = true;
    });
  }

  _pullNextUpdate() {
    if (this._isClosed || this._isDestroying || this._readInFlight) {
      return;
    }

    this._readInFlight = true;

    (this._napiDuplexStream as napi.DuplexStream)
      .read()
      .then((update) => {
        this._readInFlight = false;

        if (this._isClosed || this._isDestroying) {
          return;
        }

        const grpcUpdate = fromJsSubscribeUpdate(update);

        // Respect backpressure: only pull again if consumer accepted push.
        const canContinue = this.push(grpcUpdate);
        if (canContinue) {
          this._pullNextUpdate();
        }
      })
      .catch((err) => {
        this._readInFlight = false;

        if (this._isClosed || this._isDestroying) {
          return;
        }

        if (this._terminalErrorSeen) {
          return;
        }
        this._terminalErrorSeen = true;

        this.push(null); // Signal end of stream
        this.destroy(err as Error); // Handle resource cleanup once
      });
  }

  _read(_size: number) {
    this._pullNextUpdate();
  }

  _destroy(error: Error | null, callback: (error?: Error | null) => void) {
    // Mark terminal state first so late read completions are ignored.
    this._isDestroying = true;
    this._isClosed = true;
    this._terminalErrorSeen = true;

    // Explicitly stop the native worker so it does not outlive JS stream state.
    try {
      const nativeStream = this._napiDuplexStream as { close?: () => void };
      if (typeof nativeStream.close === "function") {
        nativeStream.close();
      }
    } catch {}

    callback(error);
  }

  _write(
    chunk: SubscribeRequest,
    _encoding: BufferEncoding,
    callback: (error?: Error | null) => void,
  ) {
    if (this._isClosed || this._isDestroying) {
      callback(new Error("Cannot write to a closed subscription stream"));
      return;
    }

    try {
      (this._napiDuplexStream as napi.DuplexStream).write(
        chunk as unknown as napi.JsSubscribeRequest,
      );
      callback();
    } catch (err) {
      callback(err as Error);
    }
  }
}

export const txEncode = {
  encoding: (napi as any).WasmUiTransactionEncoding,
  encode_raw: napi.encodeTx,
  encode: <T extends napi.WasmUiTransactionEncoding>(
    message: SubscribeUpdateTransactionInfo,
    encoding: T,
    max_supported_transaction_version: number | undefined,
    show_rewards: boolean,
  ): MapTransactionEncodingToReturnType[T] => {
    return JSON.parse(
      napi.encodeTx(
        SubscribeUpdateTransactionInfo.encode(message).finish(),
        encoding,
        max_supported_transaction_version,
        show_rewards,
      ),
    );
  },
};

export const txErrDecode = {
  decode_raw: napi.decodeTxError,
  decode: (buf: Uint8Array): TransactionErrorSolana => {
    return JSON.parse(napi.decodeTxError(Array.from(buf)));
  },
};
