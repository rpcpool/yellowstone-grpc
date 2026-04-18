package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"io"
	"log/slog"
	"os"
	"time"

	client "github.com/rpcpool/yellowstone-grpc/yellowstone-grpc-client-go"
	pb "github.com/rpcpool/yellowstone-grpc/yellowstone-grpc-client-go/proto"
)

var (
	grpcAddr           = flag.String("endpoint", "", "Solana gRPC address, in URI format e.g. https://api.rpcpool.com")
	token              = flag.String("x-token", "", "Token for authenticating")
	jsonInput          = flag.String("json", "", "JSON for subscription request, prefix with @ to read json from file")
	insecureConnection = flag.Bool("insecure", false, "Connect without TLS")
	slots              = flag.Bool("slots", false, "Subscribe to slots update")
	blocks             = flag.Bool("blocks", false, "Subscribe to block update")
	blockMeta          = flag.Bool("blocks-meta", false, "Subscribe to block metadata update")
	signature          = flag.String("signature", "", "Subscribe to a specific transaction signature")
	resub              = flag.Uint("resub", 0, "Resubscribe to only slots after x updates, 0 disables this")

	accounts = flag.Bool("accounts", false, "Subscribe to accounts")

	transactions       = flag.Bool("transactions", false, "Subscribe to transactions, required for tx_account_include/tx_account_exclude and vote/failed.")
	voteTransactions   = flag.Bool("transactions-vote", false, "Include vote transactions")
	failedTransactions = flag.Bool("transactions-failed", false, "Include failed transactions")

	accountsFilter              arrayFlags
	accountOwnersFilter         arrayFlags
	transactionsAccountsInclude arrayFlags
	transactionsAccountsExclude arrayFlags
)

func main() {
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, nil)))

	flag.Var(&accountsFilter, "accounts-account", "Subscribe to an account, may be specified multiple times to subscribe to multiple accounts.")
	flag.Var(&accountOwnersFilter, "accounts-owner", "Subscribe to an account owner, may be specified multiple times to subscribe to multiple account owners.")
	flag.Var(&transactionsAccountsInclude, "transactions-account-include", "Subscribe to transactions mentioning an account, may be specified multiple times to subscribe to multiple accounts.")
	flag.Var(&transactionsAccountsExclude, "transactions-account-exclude", "Subscribe to transactions not mentioning an account, may be specified multiple times to exclude multiple accounts.")

	flag.Parse()

	if *grpcAddr == "" {
		fatal("--endpoint is required")
	}

	b := client.NewBuilder(*grpcAddr).WithKeepaliveParams(client.DefaultKeepalive())
	if *token != "" {
		b = b.WithXToken(*token)
	}
	if *insecureConnection {
		b = b.WithInsecure()
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	c, err := b.Connect(ctx)
	if err != nil {
		fatal("connect", "err", err)
	}
	defer c.Close()

	subscription, err := buildSubscription()
	if err != nil {
		fatal("build subscription", "err", err)
	}

	subscriptionJSON, err := json.Marshal(subscription)
	if err != nil {
		fatal("marshal subscription", "err", err)
	}
	slog.Info("subscription", "request", string(subscriptionJSON))

	stream, err := c.SubscribeOnce(ctx, subscription)
	if err != nil {
		fatal("subscribe", "err", err)
	}

	var i uint = 0
	for {
		i++
		if i == *resub {
			slotsOnly := &pb.SubscribeRequest{
				Slots: map[string]*pb.SubscribeRequestFilterSlots{"slots": {}},
			}
			if err := stream.Send(slotsOnly); err != nil {
				fatal("resubscribe", "err", err)
			}
		}

		resp, err := stream.Recv()
		timestamp := time.Now().UnixNano()

		if errors.Is(err, io.EOF) {
			return
		}
		if err != nil {
			fatal("recv", "err", err)
		}

		slog.Info("update", "ts", timestamp, "update", resp)
	}
}

func buildSubscription() (*pb.SubscribeRequest, error) {
	subscription := &pb.SubscribeRequest{}

	if *jsonInput != "" {
		data := []byte(*jsonInput)
		if (*jsonInput)[0] == '@' {
			raw, err := os.ReadFile((*jsonInput)[1:])
			if err != nil {
				return nil, err
			}
			data = raw
		}
		if err := json.Unmarshal(data, subscription); err != nil {
			return nil, err
		}
	}

	// Map entries from JSON are preserved; the flags below override any
	// map key collisions.
	if *slots {
		if subscription.Slots == nil {
			subscription.Slots = map[string]*pb.SubscribeRequestFilterSlots{}
		}
		subscription.Slots["slots"] = &pb.SubscribeRequestFilterSlots{}
	}

	if *blocks {
		if subscription.Blocks == nil {
			subscription.Blocks = map[string]*pb.SubscribeRequestFilterBlocks{}
		}
		subscription.Blocks["blocks"] = &pb.SubscribeRequestFilterBlocks{}
	}

	if *blockMeta {
		if subscription.BlocksMeta == nil {
			subscription.BlocksMeta = map[string]*pb.SubscribeRequestFilterBlocksMeta{}
		}
		subscription.BlocksMeta["block_meta"] = &pb.SubscribeRequestFilterBlocksMeta{}
	}

	if (len(accountsFilter)+len(accountOwnersFilter)) > 0 || *accounts {
		if subscription.Accounts == nil {
			subscription.Accounts = map[string]*pb.SubscribeRequestFilterAccounts{}
		}
		subscription.Accounts["account_sub"] = &pb.SubscribeRequestFilterAccounts{
			Account: accountsFilter,
			Owner:   accountOwnersFilter,
		}
	}

	if *signature != "" {
		if subscription.Transactions == nil {
			subscription.Transactions = map[string]*pb.SubscribeRequestFilterTransactions{}
		}
		tr := true
		subscription.Transactions["signature_sub"] = &pb.SubscribeRequestFilterTransactions{
			Failed:    &tr,
			Vote:      &tr,
			Signature: signature,
		}
	}

	if *transactions {
		if subscription.Transactions == nil {
			subscription.Transactions = map[string]*pb.SubscribeRequestFilterTransactions{}
		}
		subscription.Transactions["transactions_sub"] = &pb.SubscribeRequestFilterTransactions{
			Failed:         failedTransactions,
			Vote:           voteTransactions,
			AccountInclude: transactionsAccountsInclude,
			AccountExclude: transactionsAccountsExclude,
		}
	}

	return subscription, nil
}

func fatal(msg string, args ...any) {
	slog.Error(msg, args...)
	os.Exit(1)
}
