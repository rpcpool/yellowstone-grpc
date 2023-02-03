package main

import (
	"context"
	"crypto/md5"
	"encoding/base64"
	"flag"
	"io"
	"log"
	"sync"
	"time"

	"github.com/gagliardetto/solana-go"
	"github.com/gagliardetto/solana-go/rpc"

	pb "github.com/solana-geyser-grpc/golang/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/credentials/insecure"
)

var (
	grpcAddr    = flag.String("grpc", "", "Solana gRPC address")
	account     = flag.String("account", "", "Account to subscribe to")
	token       = flag.String("token", "", "set token")
	accountData = flag.Bool("account-data", true, "Include data")

	grpc_slot map[uint64]uint = make(map[uint64]uint)
)

var kacp = keepalive.ClientParameters{
	Time:                10 * time.Second, // send pings every 10 seconds if there is no activity
	Timeout:             time.Second,      // wait 1 second for ping ack before considering the connection dead
	PermitWithoutStream: true,             // send pings even without active streams
}


func main() {
	log.SetFlags(0)
	flag.Parse()

  grpc_client()
}

func grpc_client() {
  log.Println("Starting grpc client")
	conn, err := grpc.Dial(*grpcAddr, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithKeepaliveParams(kacp))
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	client := pb.NewGeyserClient(conn)

	subr2 := pb.SubscribeRequest{}
	subr2.Accounts = make(map[string]*pb.SubscribeRequestFilterAccounts)
	subr2.Accounts["subscription"] = &pb.SubscribeRequestFilterAccounts{Account: []string{*account}}

  ctx := context.Background()
  if *token != "" {
    md := metadata.New(map[string]string{"x-token": *token})
    ctx = metadata.NewOutgoingContext(ctx, md)
  }

	stream, err := client.Subscribe(ctx, &subr2)
	if err != nil {
		panic(err)
	}
	for {
		resp, err := stream.Recv()
		timestamp := time.Now().UnixNano()

		if err == io.EOF {
			return
		}
		if err != nil {
			panic(err)
		}

		account := resp.GetAccount()

		grpc_slot[account.Slot]++

		if *accountData {
			log.Printf("[GRPC] %d{%d}: %s @ %d", account.Slot, grpc_slot[account.Slot], base64.StdEncoding.EncodeToString(account.Account.Data), timestamp)
		} else {
			log.Printf("[GRPC] %d{%d}: %x @ %d", account.Slot, grpc_slot[account.Slot], md5.Sum(account.Account.Data), timestamp)
		}
	}
}

