from typing import Optional
import grpc
import geyser_pb2
import geyser_pb2_grpc
import logging
import click



def _triton_sign_request(
    callback: grpc.AuthMetadataPluginCallback,
    x_token: Optional[str],
    error: Optional[Exception],
):
    # WARNING: metadata is a 1d-tuple (<value>,), the last comma is necessary
    metadata = (("x-token", x_token),)
    return callback(metadata, error)


class TritonAuthMetadataPlugin(grpc.AuthMetadataPlugin):
    """Metadata wrapper for raw access token credentials."""

    def __init__(self, x_token: str):
        self.x_token = x_token

    def __call__(
        self,
        context: grpc.AuthMetadataContext,
        callback: grpc.AuthMetadataPluginCallback,
    ):
        return _triton_sign_request(callback, self.x_token, None)


@click.command()
@click.option('--rpc-fqdn', help='Fully Qualified domain name of your RPC endpoint')
@click.option('--x-token', help='x-token to authenticate each gRPC call')
def helloworld_geyser(rpc_fqdn, x_token):
    """Simple program to get the latest solana slot number"""
    auth = TritonAuthMetadataPlugin(x_token)
    # ssl_creds allow you to use our https endpoint
    # grpc.ssl_channel_credentials with no arguments will look through your CA trust store.
    ssl_creds = grpc.ssl_channel_credentials()

    # call credentials will be sent on each request if setup with composite_channel_credentials.
    call_creds: grpc.CallCredentials = grpc.metadata_call_credentials(auth)

    # Combined creds will store the channel creds aswell as the call credentials
    combined_creds = grpc.composite_channel_credentials(ssl_creds, call_creds)

    with grpc.secure_channel(rpc_fqdn, credentials=combined_creds) as channel:
        stub = geyser_pb2_grpc.GeyserStub(channel)
        response = stub.GetSlot(geyser_pb2.GetSlotRequest())
        print(response)

if __name__ == '__main__':
    logging.basicConfig()
    helloworld_geyser()