package main

import (
	"context"
	"crypto/x509"
	"encoding/json"
	"flag"
	"io"
	"log"
	"os"
	"time"

	pb "github.com/rpcpool/solana-geyser-grpc/golang/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/metadata"
)

var (
	grpcAddr           = flag.String("grpc", "", "Solana gRPC address")
	token              = flag.String("token", "", "set token")
	jsonInput          = flag.String("json", "", "JSON for subscription request, prefix with @ to read json from file")
	insecureConnection = flag.Bool("insecure", false, "Connect without TLS")
	slots              = flag.Bool("slots", false, "Subscribe to slots update")
	blocks             = flag.Bool("blocks", false, "Subscribe to block update")
	block_meta         = flag.Bool("block_meta", false, "Subscribe to block metadata update")
	signature          = flag.String("signature", "", "Subscribe to a specific transaction signature")

	transactions       = flag.Bool("transactions", false, "Subscribe to transactions, required for tx_account_include/tx_account_exclude and vote/failed.")
	voteTransactions   = flag.Bool("vote", false, "Include vote transactions")
	failedTransactions = flag.Bool("failed", false, "Include failed transactions")

	accountsFilter              arrayFlags
	accountOwnersFilter         arrayFlags
	transactionsAccountsInclude arrayFlags
	transactionsAccountsExclude arrayFlags
)

var kacp = keepalive.ClientParameters{
	Time:                10 * time.Second, // send pings every 10 seconds if there is no activity
	Timeout:             time.Second,      // wait 1 second for ping ack before considering the connection dead
	PermitWithoutStream: true,             // send pings even without active streams
}

func main() {
	log.SetFlags(0)

	flag.Var(&accountsFilter, "account", "Subscribe to an account, may be specified multiple times to subscribe to multiple accounts.")
	flag.Var(&accountOwnersFilter, "owner", "Subscribe to an account owner, may be specified multiple times to subscribe to multiple account owners.")
	flag.Var(&transactionsAccountsInclude, "tx_account_include", "Subscribe to transactions mentioning an account, may be specified multiple times to subscribe to multiple accounts.")
	flag.Var(&transactionsAccountsExclude, "tx_account_exclude", "Subscribe to transactions not mentioning an account, may be specified multiple times to exclude multiple accounts.")

	flag.Parse()

	grpc_client()
}

func grpc_client() {
	var opts []grpc.DialOption
	if *insecureConnection {
		opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	} else {
		pool, _ := x509.SystemCertPool()
		creds := credentials.NewClientTLSFromCert(pool, "")
		opts = append(opts, grpc.WithTransportCredentials(creds))
	}

	opts = append(opts, grpc.WithKeepaliveParams(kacp))

	log.Println("Starting grpc client")
	conn, err := grpc.Dial(*grpcAddr, opts...)
	if err != nil {
		log.Fatalf("fail to dial: %v", err)
	}
	defer conn.Close()

	client := pb.NewGeyserClient(conn)

	var subscription pb.SubscribeRequest

	// Read json input or JSON file prefixed with @
	if *jsonInput != "" {
		var jsonData []byte
		if (*jsonInput)[0] == '@' {
			jsonData, err = os.ReadFile((*jsonInput)[1:])
			if err != nil {
				log.Fatalf("Error reading provided json file: %v", err)
			}
		} else {
			jsonData = []byte(*jsonInput)
		}
		err := json.Unmarshal(jsonData, &subscription)
		if err != nil {
			log.Fatalf("Error parsing JSON: %v", err)
		}
	} else {
		// If no JSON provided, start with blank
		subscription = pb.SubscribeRequest{}
	}

	// We overwrite the json provided if command line arguments are specified
	if *slots {
		subscription.Slots = make(map[string]*pb.SubscribeRequestFilterSlots)
		subscription.Slots["slots"] = &pb.SubscribeRequestFilterSlots{}

	}

	if *blocks {
		subscription.Blocks = make(map[string]*pb.SubscribeRequestFilterBlocks)
		subscription.Blocks["blocks"] = &pb.SubscribeRequestFilterBlocks{}
	}

	if *block_meta {
		subscription.BlocksMeta = make(map[string]*pb.SubscribeRequestFilterBlocksMeta)
		subscription.BlocksMeta["block_meta"] = &pb.SubscribeRequestFilterBlocksMeta{}
	}

	if (len(accountsFilter) + len(accountOwnersFilter)) > 0 {
		subscription.Accounts = make(map[string]*pb.SubscribeRequestFilterAccounts)
		subscription.Accounts["account_sub"] = &pb.SubscribeRequestFilterAccounts{
			Account: accountsFilter,
			Owner:   accountOwnersFilter,
		}
	}

	// Set up the transactions subscription
	subscription.Transactions = make(map[string]*pb.SubscribeRequestFilterTransactions)

	// Subscribe to a specific signature
	if *signature != "" {
		tr := true
		subscription.Transactions["signature_sub"] = &pb.SubscribeRequestFilterTransactions{
			Failed: &tr,
			Vote:   &tr,
		}

		if *signature != "" {
			subscription.Transactions["signature_sub"].Signature = signature
		}
	}

	// Subscribe to generic transaction stream
	if *transactions {

		subscription.Transactions["transactions_sub"] = &pb.SubscribeRequestFilterTransactions{
			Failed: failedTransactions,
			Vote:   voteTransactions,
		}

		subscription.Transactions["transactions_sub"].AccountInclude = transactionsAccountsInclude
		subscription.Transactions["transactions_sub"].AccountExclude = transactionsAccountsExclude
	}

	subscriptionJson, err := json.Marshal(&subscription)
	if err != nil {
		log.Printf("Failed to marshal subscription request: %v", subscriptionJson)
	}
	log.Printf("Subscription request: %s", string(subscriptionJson))

	// Set up the subscription request
	ctx := context.Background()
	if *token != "" {
		md := metadata.New(map[string]string{"x-token": *token})
		ctx = metadata.NewOutgoingContext(ctx, md)
	}

	stream, err := client.Subscribe(ctx)
	if err != nil {
		log.Fatalf("%v", err)
	}
	err = stream.Send(&subscription)
	if err != nil {
		log.Fatalf("%v", err)
	}
	stream.CloseSend()

	for {
		resp, err := stream.Recv()
		timestamp := time.Now().UnixNano()

		if err == io.EOF {
			return
		}
		if err != nil {
			log.Fatalf("Error occurred in receiving update: %v", err)
		}

		log.Printf("%v %v", timestamp, resp)
	}
}
