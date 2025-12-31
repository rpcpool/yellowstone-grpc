/** TypeScript/JavaScript client for gRPC Geyser. */

// Import generated gRPC client and types.
import { SubscribeUpdateTransactionInfo } from "./grpc/geyser";

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
    commitment?: number,
  ): Promise<napi.JsGetLatestBlockhashResponse> {
    if (!this._grpcClient) {
      throw new Error("Client not connected. Call connect() first");
    }

    const request: napi.JsGetLatestBlockhashRequest = {
      commitment: commitment ?? null,
    };

    return await this._grpcClient.getLatestBlockhash(request);
  }

  async ping(count: number): Promise<number> {
    if (!this._grpcClient) {
      throw new Error("Client not connected. Call connect() first");
    }

    const request: napi.JsPingRequest = {
      count,
    };

    return (await this._grpcClient.ping(request)).count;
  }

  async getBlockHeight(commitment?: number): Promise<string> {
    if (!this._grpcClient) {
      throw new Error("Client not connected. Call connect() first");
    }

    const request: napi.JsGetBlockHeightRequest = {
      commitment,
    };

    return (await this._grpcClient.getBlockHeight(request)).blockHeight;
  }

  async getSlot(commitment?: number): Promise<string> {
    if (!this._grpcClient) {
      throw new Error("Client not connected. Call connect() first");
    }

    const request: napi.JsGetSlotRequest = {
      commitment,
    };

    return (await this._grpcClient.getSlot(request)).slot;
  }

  async isBlockhashValid(
    blockhash: string,
    commitment?: number,
  ): Promise<napi.JsIsBlockhashValidResponse> {
    if (!this._grpcClient) {
      throw new Error("Client not connected. Call connect() first");
    }

    const request: napi.JsIsBlockhashValidRequest = {
      blockhash,
      commitment,
    };

    return await this._grpcClient.isBlockhashValid(request);
  }

  async getVersion(): Promise<string> {
    if (!this._grpcClient) {
      throw new Error("Client not connected. Call connect() first");
    }

    const request: napi.JsGetVersionRequest = {};

    return (await this._grpcClient.getVersion(request)).version;
  }

  async subscribeReplayInfo(): Promise<napi.JsSubscribeReplayInfoResponse> {
    if (!this._grpcClient) {
      throw new Error("Client not connected. Call connect() first");
    }

    const request: napi.JsSubscribeReplayInfoRequest = {};

    return await this._grpcClient.subscribeReplayInfo(request);
  }

  async subscribe(): Promise<ClientDuplexStream> {
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

  constructor(stream: napi.DuplexStream, options: object | undefined) {
    super({ ...options });
    this._napiDuplexStream = stream;
  }

  async _read(_size: number) {
    try {
      const update = await this._napiDuplexStream.read();
      this.push(update);
    } catch (err) {
      this.push(null); // Signal end of stream
      this.destroy(err); // Handle resource cleanup
    }
  }

  _write(chunk: object, _encoding: any, callback: any) {
    try {
      this._napiDuplexStream.write(chunk);
      callback();
    } catch (err) {
      callback(err);
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
