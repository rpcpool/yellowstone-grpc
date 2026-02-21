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
import { JsSubscribeRequest } from "../napi";

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
  _insecureEndpoint: string;
  _insecureXToken: string | undefined;
  _channelOptions: napi.JsChannelOptions | undefined;
  _grpcClient: napi.GrpcClient | null = null;

  constructor(
    endpoint: string,
    xToken: string | undefined,
    channel_options: napi.JsChannelOptions | undefined,
  ) {
    this._insecureEndpoint = endpoint;
    this._insecureXToken = xToken;
    this._channelOptions = channel_options;
  }

  async connect(): Promise<void> {
    // Use the factory method to create the client
    this._grpcClient = await napi.GrpcClient.new(
      this._insecureEndpoint,
      this._insecureXToken,
      this._channelOptions,
    );
  }

  async getLatestBlockhash(
    commitment?: CommitmentLevel,
  ): Promise<GetLatestBlockhashResponse> {
    if (!this._grpcClient) {
      throw new Error("Client not connected. Call connect() first");
    }

    const request: napi.JsGetLatestBlockhashRequest = {
      commitment: commitment ?? null,
    };

    const response = await this._grpcClient.getLatestBlockhash(request);
    return GetLatestBlockhashResponseMessage.fromPartial({
      slot: response.slot,
      blockhash: response.blockhash,
      lastValidBlockHeight: response.lastValidBlockHeight,
    });
  }

  async ping(count: number): Promise<PongResponse> {
    if (!this._grpcClient) {
      throw new Error("Client not connected. Call connect() first");
    }

    const request: napi.JsPingRequest = {
      count,
    };

    const response = await this._grpcClient.ping(request);
    return PongResponseMessage.fromPartial({
      count: response.count,
    });
  }

  async getBlockHeight(commitment?: CommitmentLevel): Promise<GetBlockHeightResponse> {
    if (!this._grpcClient) {
      throw new Error("Client not connected. Call connect() first");
    }

    const request: napi.JsGetBlockHeightRequest = {
      commitment,
    };

    const response = await this._grpcClient.getBlockHeight(request);
    return GetBlockHeightResponseMessage.fromPartial({
      blockHeight: response.blockHeight,
    });
  }

  async getSlot(commitment?: CommitmentLevel): Promise<GetSlotResponse> {
    if (!this._grpcClient) {
      throw new Error("Client not connected. Call connect() first");
    }

    const request: napi.JsGetSlotRequest = {
      commitment,
    };

    const response = await this._grpcClient.getSlot(request);
    return GetSlotResponseMessage.fromPartial({
      slot: response.slot,
    });
  }

  async isBlockhashValid(
    blockhash: string,
    commitment?: CommitmentLevel,
  ): Promise<IsBlockhashValidResponse> {
    if (!this._grpcClient) {
      throw new Error("Client not connected. Call connect() first");
    }

    const request: napi.JsIsBlockhashValidRequest = {
      blockhash,
      commitment,
    };

    const response = await this._grpcClient.isBlockhashValid(request);
    return IsBlockhashValidResponseMessage.fromPartial({
      slot: response.slot,
      valid: response.valid,
    });
  }

  async getVersion(): Promise<GetVersionResponse> {
    if (!this._grpcClient) {
      throw new Error("Client not connected. Call connect() first");
    }

    const request: napi.JsGetVersionRequest = {};

    const response = await this._grpcClient.getVersion(request);
    return GetVersionResponseMessage.fromPartial({
      version: response.version,
    });
  }

  async subscribeReplayInfo(): Promise<SubscribeReplayInfoResponse> {
    if (!this._grpcClient) {
      throw new Error("Client not connected. Call connect() first");
    }

    const request: napi.JsSubscribeReplayInfoRequest = {};

    const response = await this._grpcClient.subscribeReplayInfo(request);
    return SubscribeReplayInfoResponseMessage.fromPartial({
      firstAvailable: response.firstAvailable,
    });
  }

  async subscribe(): Promise<ClientDuplexStream> {
    if (!this._grpcClient) {
      throw new Error("Client not connected. Call connect() first");
    }

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

    const stream = this._grpcClient.subscribe();

    return new Promise<ClientDuplexStream>((resolve, reject) => {
      try {
        resolve(new ClientDuplexStream(stream, options));
      } catch (err) {
        reject(err);
      }
    });
  }
}

class ClientDuplexStream extends Duplex {
  _napiDuplexStream: napi.DuplexStream;
  _readInFlight: boolean;
  _isClosed: boolean;
  _isDestroying: boolean;
  _terminalErrorSeen: boolean;

  constructor(stream: napi.DuplexStream, options: object | undefined) {
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

    this._napiDuplexStream
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
    this._isDestroying = true;
    this._isClosed = true;
    this._terminalErrorSeen = true;
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
      this._napiDuplexStream.write(chunk as JsSubscribeRequest);
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
