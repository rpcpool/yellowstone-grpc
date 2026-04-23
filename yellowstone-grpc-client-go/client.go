// Package yellowstonegrpc provides a Go client for the Yellowstone Geyser gRPC
// interface. It is the Go-side counterpart to yellowstone-grpc-client-nodejs
// and yellowstone-grpc-client (Rust): a thin wrapper over the generated gRPC
// client that handles connection setup, x-token authentication, and the two
// bidirectional subscription streams (regular + deshred).
package yellowstonegrpc

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net"
	"net/url"
	"strings"
	"time"

	pb "github.com/rpcpool/yellowstone-grpc/yellowstone-grpc-client-go/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/encoding/gzip"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/metadata"
)

// healthServiceName is the service identifier used for Geyser's gRPC
// health checks. Matches the Rust client's `health_check` / `health_watch`.
const healthServiceName = "geyser.Geyser"

// ErrConnectionShutdown is returned by WaitReady when the underlying
// gRPC connection transitions to Shutdown before reaching Ready.
var ErrConnectionShutdown = errors.New("yellowstonegrpc: connection shut down before reaching ready")

// Client is a Yellowstone Geyser gRPC client.
//
// A Client owns a single *grpc.ClientConn reused across all calls. Call
// Close when done to release the connection.
type Client struct {
	conn   *grpc.ClientConn
	geyser pb.GeyserClient
	health healthpb.HealthClient
}

// Geyser returns the underlying generated gRPC client. Prefer the typed
// helpers on Client, but this escape hatch exists for callers that need
// gRPC CallOptions (e.g. per-call deadlines, compression) not exposed here.
func (c *Client) Geyser() pb.GeyserClient { return c.geyser }

// Health returns the underlying grpc.health HealthClient.
func (c *Client) Health() healthpb.HealthClient { return c.health }

// Conn returns the underlying *grpc.ClientConn.
func (c *Client) Conn() *grpc.ClientConn { return c.conn }

// Close tears down the gRPC connection.
func (c *Client) Close() error { return c.conn.Close() }

// WaitReady blocks until the connection reaches connectivity.Ready or ctx
// expires. grpc.NewClient does not dial until the first RPC — call this
// when you want to fail fast on connection errors at startup. Mirrors the
// Rust builder's eager `connect()` semantics.
func (c *Client) WaitReady(ctx context.Context) error {
	c.conn.Connect()
	for {
		state := c.conn.GetState()
		if state == connectivity.Ready {
			return nil
		}
		if state == connectivity.Shutdown {
			return ErrConnectionShutdown
		}
		if !c.conn.WaitForStateChange(ctx, state) {
			return ctx.Err()
		}
	}
}

// HealthCheck invokes the grpc.health Check RPC against the Geyser service.
func (c *Client) HealthCheck(ctx context.Context) (*healthpb.HealthCheckResponse, error) {
	return c.health.Check(ctx, &healthpb.HealthCheckRequest{Service: healthServiceName})
}

// HealthWatch opens the grpc.health Watch stream for the Geyser service.
func (c *Client) HealthWatch(ctx context.Context) (grpc.ServerStreamingClient[healthpb.HealthCheckResponse], error) {
	return c.health.Watch(ctx, &healthpb.HealthCheckRequest{Service: healthServiceName})
}

// Ping invokes the Ping RPC.
func (c *Client) Ping(ctx context.Context, count int32) (*pb.PongResponse, error) {
	return c.geyser.Ping(ctx, &pb.PingRequest{Count: count})
}

// GetLatestBlockhash invokes the GetLatestBlockhash RPC.
// Pass nil for commitment to let the server choose the default.
func (c *Client) GetLatestBlockhash(ctx context.Context, commitment *pb.CommitmentLevel) (*pb.GetLatestBlockhashResponse, error) {
	return c.geyser.GetLatestBlockhash(ctx, &pb.GetLatestBlockhashRequest{Commitment: commitment})
}

// GetBlockHeight invokes the GetBlockHeight RPC.
func (c *Client) GetBlockHeight(ctx context.Context, commitment *pb.CommitmentLevel) (*pb.GetBlockHeightResponse, error) {
	return c.geyser.GetBlockHeight(ctx, &pb.GetBlockHeightRequest{Commitment: commitment})
}

// GetSlot invokes the GetSlot RPC.
func (c *Client) GetSlot(ctx context.Context, commitment *pb.CommitmentLevel) (*pb.GetSlotResponse, error) {
	return c.geyser.GetSlot(ctx, &pb.GetSlotRequest{Commitment: commitment})
}

// IsBlockhashValid invokes the IsBlockhashValid RPC.
func (c *Client) IsBlockhashValid(ctx context.Context, blockhash string, commitment *pb.CommitmentLevel) (*pb.IsBlockhashValidResponse, error) {
	return c.geyser.IsBlockhashValid(ctx, &pb.IsBlockhashValidRequest{
		Blockhash:  blockhash,
		Commitment: commitment,
	})
}

// GetVersion invokes the GetVersion RPC.
func (c *Client) GetVersion(ctx context.Context) (*pb.GetVersionResponse, error) {
	return c.geyser.GetVersion(ctx, &pb.GetVersionRequest{})
}

// SubscribeReplayInfo invokes the SubscribeReplayInfo RPC.
func (c *Client) SubscribeReplayInfo(ctx context.Context) (*pb.SubscribeReplayInfoResponse, error) {
	return c.geyser.SubscribeReplayInfo(ctx, &pb.SubscribeReplayInfoRequest{})
}

// Subscribe opens the Geyser Subscribe bidi stream without sending an
// initial request. Send SubscribeRequest(s) via stream.Send; receive
// SubscribeUpdate(s) via stream.Recv.
func (c *Client) Subscribe(ctx context.Context, opts ...grpc.CallOption) (pb.Geyser_SubscribeClient, error) {
	return c.geyser.Subscribe(ctx, opts...)
}

// SubscribeOnce opens the Subscribe stream and immediately sends req.
// Use when you do not need to change the subscription after opening.
//
// ctx governs the lifetime of the returned stream — pass a cancellable
// context and cancel it when done, otherwise the stream will live until
// Client.Close is called.
func (c *Client) SubscribeOnce(ctx context.Context, req *pb.SubscribeRequest, opts ...grpc.CallOption) (pb.Geyser_SubscribeClient, error) {
	stream, err := c.geyser.Subscribe(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("open subscribe stream: %w", err)
	}
	if req != nil {
		if err := stream.Send(req); err != nil {
			// Half-close signals the server we will not send more; the
			// caller is expected to cancel ctx to release the client side.
			_ = stream.CloseSend()
			return nil, fmt.Errorf("send initial subscribe request: %w", err)
		}
	}
	return stream, nil
}

// SubscribeDeshred opens the SubscribeDeshred bidi stream.
//
// Deshredded updates are emitted at the earliest possible point — before
// replay — so they do not carry replay metadata (status, logs, CU used).
func (c *Client) SubscribeDeshred(ctx context.Context, opts ...grpc.CallOption) (pb.Geyser_SubscribeDeshredClient, error) {
	return c.geyser.SubscribeDeshred(ctx, opts...)
}

// SubscribeDeshredOnce opens SubscribeDeshred and immediately sends req.
// See SubscribeOnce for context-lifetime semantics.
func (c *Client) SubscribeDeshredOnce(ctx context.Context, req *pb.SubscribeDeshredRequest, opts ...grpc.CallOption) (pb.Geyser_SubscribeDeshredClient, error) {
	stream, err := c.geyser.SubscribeDeshred(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("open deshred stream: %w", err)
	}
	if req != nil {
		if err := stream.Send(req); err != nil {
			_ = stream.CloseSend()
			return nil, fmt.Errorf("send initial deshred request: %w", err)
		}
	}
	return stream, nil
}

// Builder configures and constructs a Client.
//
// Matches the shape of the Rust GeyserGrpcBuilder — a fluent configuration
// object that is finalized with Connect or ConnectUDS. All setters return
// the receiver so they chain, e.g.:
//
//	c, err := NewBuilder("https://api.rpcpool.com").
//	    WithXToken("my-token").
//	    WithKeepaliveParams(keepalive.ClientParameters{Time: 10 * time.Second}).
//	    Connect(ctx)
type Builder struct {
	endpoint         string
	xToken           string
	xRequestSnapshot bool

	forcePlaintext bool

	tlsConfig *tls.Config

	keepaliveSet    bool
	keepaliveParams keepalive.ClientParameters

	maxRecvMsgSize int
	maxSendMsgSize int

	initialConnWindowSize int32
	initialWindowSize     int32
	readBufferSize        int
	writeBufferSize       int

	tcpKeepalive *time.Duration
	tcpNoDelay   *bool

	useGzip bool

	disableVTCodec bool

	extraDialOptions []grpc.DialOption
}

// NewBuilder creates a Builder for the given endpoint. The endpoint may
// be a bare host:port or a URL like "https://host:port". For URL form, the
// scheme determines TLS (http = insecure, https = TLS) and the port is
// inferred from scheme if absent.
func NewBuilder(endpoint string) *Builder {
	return &Builder{endpoint: endpoint}
}

// WithXToken sets the x-token sent in outgoing request metadata.
func (b *Builder) WithXToken(token string) *Builder {
	b.xToken = token
	return b
}

// WithRequestSnapshot toggles the x-request-snapshot metadata header on
// every outgoing request. When enabled, the server is asked to replay
// an initial account snapshot before streaming live updates — relevant
// mainly for account subscriptions.
func (b *Builder) WithRequestSnapshot(v bool) *Builder {
	b.xRequestSnapshot = v
	return b
}

// WithInsecure forces plaintext TCP. Takes precedence over WithTLSConfig
// and over the scheme inferred from the endpoint URL.
func (b *Builder) WithInsecure() *Builder {
	b.forcePlaintext = true
	return b
}

// WithTLSConfig sets an explicit *tls.Config. If not called, the system
// root CAs are used on TLS connections.
func (b *Builder) WithTLSConfig(cfg *tls.Config) *Builder {
	b.tlsConfig = cfg
	return b
}

// WithKeepaliveParams sets gRPC (HTTP/2) client keepalive parameters —
// ping interval, ping timeout, and whether to ping when idle.
func (b *Builder) WithKeepaliveParams(kp keepalive.ClientParameters) *Builder {
	b.keepaliveSet = true
	b.keepaliveParams = kp
	return b
}

// WithMaxDecodingMessageSize sets the maximum inbound message size in bytes.
func (b *Builder) WithMaxDecodingMessageSize(n int) *Builder {
	b.maxRecvMsgSize = n
	return b
}

// WithMaxEncodingMessageSize sets the maximum outbound message size in bytes.
func (b *Builder) WithMaxEncodingMessageSize(n int) *Builder {
	b.maxSendMsgSize = n
	return b
}

// WithInitialConnWindowSize sets the HTTP/2 initial connection window
// size (grpc.WithInitialConnWindowSize). Values below 64KiB are ignored.
func (b *Builder) WithInitialConnWindowSize(n int32) *Builder {
	b.initialConnWindowSize = n
	return b
}

// WithInitialWindowSize sets the HTTP/2 initial per-stream window size
// (grpc.WithInitialWindowSize). Values below 64KiB are ignored.
func (b *Builder) WithInitialWindowSize(n int32) *Builder {
	b.initialWindowSize = n
	return b
}

// WithReadBufferSize sets the connection read buffer size in bytes.
func (b *Builder) WithReadBufferSize(n int) *Builder {
	b.readBufferSize = n
	return b
}

// WithWriteBufferSize sets the connection write buffer size in bytes.
func (b *Builder) WithWriteBufferSize(n int) *Builder {
	b.writeBufferSize = n
	return b
}

// WithTCPKeepalive configures the TCP-level keepalive interval applied to
// new connections. Setting this installs a custom dialer — it is ignored
// when connecting over UDS.
//
// Follows net.Dialer semantics: a zero duration falls back to the OS
// default (~15s on modern Linux/macOS); a negative duration disables TCP
// keepalive entirely. Pass an explicit positive value to override.
func (b *Builder) WithTCPKeepalive(d time.Duration) *Builder {
	b.tcpKeepalive = &d
	return b
}

// WithTCPNoDelay toggles the TCP_NODELAY flag on new connections. Setting
// this installs a custom dialer — it is ignored when connecting over UDS.
func (b *Builder) WithTCPNoDelay(enabled bool) *Builder {
	b.tcpNoDelay = &enabled
	return b
}

// WithGzip enables gzip compression on outbound RPCs. The server may
// still return uncompressed responses; the client accepts gzip from the
// server automatically because the codec is registered on import.
func (b *Builder) WithGzip() *Builder {
	b.useGzip = true
	return b
}

// WithVTProtoCodec toggles the vtproto-backed gRPC codec (enabled by
// default). The codec is installed per-connection via grpc.ForceCodecV2
// and never touches the global encoding registry, so disabling it here
// affects only this Client. Pass false to fall back to grpc-go's
// built-in proto codec — useful for A/B measurement or if you hit a
// bug in the generated vtproto methods.
func (b *Builder) WithVTProtoCodec(enabled bool) *Builder {
	b.disableVTCodec = !enabled
	return b
}

// WithDialOption appends arbitrary grpc.DialOption values. Use for any
// transport option not covered by the helpers above.
func (b *Builder) WithDialOption(opts ...grpc.DialOption) *Builder {
	b.extraDialOptions = append(b.extraDialOptions, opts...)
	return b
}

// Connect resolves the endpoint, dials, and returns a Client.
//
// grpc.NewClient is lazy: the returned Client has not opened a TCP
// connection yet, and any dial/TLS errors surface on the first RPC
// (or via Client.WaitReady). The ctx parameter is reserved for future
// use and is ignored today — pass context.Background() if unsure.
func (b *Builder) Connect(_ context.Context) (*Client, error) {
	address, plaintext, err := resolveEndpoint(b.endpoint, b.forcePlaintext)
	if err != nil {
		return nil, err
	}

	opts, err := b.baseDialOptions(plaintext)
	if err != nil {
		return nil, err
	}

	if dialer := b.tcpDialer(); dialer != nil {
		opts = append(opts, grpc.WithContextDialer(dialer))
	}

	return b.newClient(address, opts)
}

// ConnectUDS dials a Unix Domain Socket at path instead of a TCP endpoint.
// The endpoint set on the Builder is ignored. TCP tuning options
// (WithTCPKeepalive / WithTCPNoDelay) are ignored — these are TCP-only.
//
// Plaintext is forced: UDS connections skip TLS even if the Builder has
// a TLS config set. Like Connect, the ctx parameter is reserved for
// future use and is ignored today.
func (b *Builder) ConnectUDS(_ context.Context, path string) (*Client, error) {
	if path == "" {
		return nil, errors.New("ConnectUDS: path is required")
	}

	opts, err := b.baseDialOptions(true) // always plaintext over UDS
	if err != nil {
		return nil, err
	}

	opts = append(opts, grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
		var d net.Dialer
		return d.DialContext(ctx, "unix", path)
	}))

	// Use the passthrough resolver so gRPC does not try to resolve the
	// target as a hostname — the dialer ignores it anyway.
	return b.newClient("passthrough:///"+path, opts)
}

// baseDialOptions assembles the transport-independent grpc.DialOptions:
// credentials, keepalive, window / buffer sizes, call-level options,
// auth interceptors, and user-provided extras. Callers append any
// transport-specific dialer on top.
func (b *Builder) baseDialOptions(plaintext bool) ([]grpc.DialOption, error) {
	var opts []grpc.DialOption

	if plaintext {
		opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	} else {
		tlsCfg := b.tlsConfig
		if tlsCfg == nil {
			pool, err := x509.SystemCertPool()
			if err != nil {
				return nil, fmt.Errorf("load system cert pool: %w", err)
			}
			tlsCfg = &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12}
		}
		opts = append(opts, grpc.WithTransportCredentials(credentials.NewTLS(tlsCfg)))
	}

	if b.keepaliveSet {
		opts = append(opts, grpc.WithKeepaliveParams(b.keepaliveParams))
	}
	if b.initialConnWindowSize > 0 {
		opts = append(opts, grpc.WithInitialConnWindowSize(b.initialConnWindowSize))
	}
	if b.initialWindowSize > 0 {
		opts = append(opts, grpc.WithInitialWindowSize(b.initialWindowSize))
	}
	if b.readBufferSize != 0 {
		opts = append(opts, grpc.WithReadBufferSize(b.readBufferSize))
	}
	if b.writeBufferSize != 0 {
		opts = append(opts, grpc.WithWriteBufferSize(b.writeBufferSize))
	}

	var callOpts []grpc.CallOption
	if b.maxRecvMsgSize > 0 {
		callOpts = append(callOpts, grpc.MaxCallRecvMsgSize(b.maxRecvMsgSize))
	}
	if b.maxSendMsgSize > 0 {
		callOpts = append(callOpts, grpc.MaxCallSendMsgSize(b.maxSendMsgSize))
	}
	if b.useGzip {
		callOpts = append(callOpts, grpc.UseCompressor(gzip.Name))
	}
	if !b.disableVTCodec {
		// Scope the vtproto codec to this ClientConn — ForceCodecV2 hands
		// the codec directly to the call path without touching the
		// package-level encoding registry, so other gRPC clients in the
		// user's process keep the stock proto codec.
		callOpts = append(callOpts, grpc.ForceCodecV2(defaultVTCodec))
	}
	if len(callOpts) > 0 {
		opts = append(opts, grpc.WithDefaultCallOptions(callOpts...))
	}

	if b.xToken != "" || b.xRequestSnapshot {
		md := metadata.MD{}
		if b.xToken != "" {
			md.Set("x-token", b.xToken)
		}
		if b.xRequestSnapshot {
			md.Set("x-request-snapshot", "true")
		}
		// Use Chain* variants so a user-supplied WithUnaryInterceptor via
		// WithDialOption cannot silently replace the auth interceptor and
		// drop the x-token header.
		opts = append(opts,
			grpc.WithChainUnaryInterceptor(metadataUnaryInterceptor(md)),
			grpc.WithChainStreamInterceptor(metadataStreamInterceptor(md)),
		)
	}

	opts = append(opts, b.extraDialOptions...)
	return opts, nil
}

// tcpDialer returns a context dialer that applies TCP-level options, or
// nil if no TCP tuning has been requested. Keeping this separate lets
// UDS ignore TCP-only options cleanly.
func (b *Builder) tcpDialer() func(context.Context, string) (net.Conn, error) {
	if b.tcpKeepalive == nil && b.tcpNoDelay == nil {
		return nil
	}
	d := &net.Dialer{}
	if b.tcpKeepalive != nil {
		d.KeepAlive = *b.tcpKeepalive
	}
	noDelay := b.tcpNoDelay
	return func(ctx context.Context, addr string) (net.Conn, error) {
		conn, err := d.DialContext(ctx, "tcp", addr)
		if err != nil {
			return nil, err
		}
		if noDelay != nil {
			if tc, ok := conn.(*net.TCPConn); ok {
				_ = tc.SetNoDelay(*noDelay)
			}
		}
		return conn, nil
	}
}

func (b *Builder) newClient(address string, opts []grpc.DialOption) (*Client, error) {
	conn, err := grpc.NewClient(address, opts...)
	if err != nil {
		return nil, fmt.Errorf("grpc.NewClient: %w", err)
	}
	return &Client{
		conn:   conn,
		geyser: pb.NewGeyserClient(conn),
		health: healthpb.NewHealthClient(conn),
	}, nil
}

// resolveEndpoint normalizes endpoint to host:port and determines whether
// to use plaintext. Inputs containing "://" are parsed as URLs and must
// have an http or https scheme; other schemes are rejected up front to
// avoid surprising failures deep inside grpc-go's name resolver. Bare
// host:port inputs (which url.Parse misinterprets as "<scheme>:<opaque>")
// are passed through as-is. forcePlaintext wins over scheme inference.
func resolveEndpoint(endpoint string, forcePlaintext bool) (address string, plaintext bool, err error) {
	if endpoint == "" {
		return "", false, errors.New("endpoint is required")
	}

	if !strings.Contains(endpoint, "://") {
		// Bare host:port form.
		return endpoint, forcePlaintext, nil
	}

	u, parseErr := url.Parse(endpoint)
	if parseErr != nil {
		return "", false, fmt.Errorf("parse endpoint %q: %w", endpoint, parseErr)
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return "", false, fmt.Errorf("unsupported URL scheme %q (expected http or https)", u.Scheme)
	}

	plaintext = forcePlaintext || u.Scheme == "http"

	host := u.Hostname()
	if host == "" {
		return "", false, fmt.Errorf("endpoint missing hostname: %q", endpoint)
	}
	port := u.Port()
	if port == "" {
		if plaintext {
			port = "80"
		} else {
			port = "443"
		}
	}
	return host + ":" + port, plaintext, nil
}

func metadataUnaryInterceptor(base metadata.MD) grpc.UnaryClientInterceptor {
	return func(
		ctx context.Context,
		method string,
		req, reply any,
		cc *grpc.ClientConn,
		invoker grpc.UnaryInvoker,
		opts ...grpc.CallOption,
	) error {
		return invoker(mergeMetadata(ctx, base), method, req, reply, cc, opts...)
	}
}

func metadataStreamInterceptor(base metadata.MD) grpc.StreamClientInterceptor {
	return func(
		ctx context.Context,
		desc *grpc.StreamDesc,
		cc *grpc.ClientConn,
		method string,
		streamer grpc.Streamer,
		opts ...grpc.CallOption,
	) (grpc.ClientStream, error) {
		return streamer(mergeMetadata(ctx, base), desc, cc, method, opts...)
	}
}

func mergeMetadata(ctx context.Context, extra metadata.MD) context.Context {
	// Fast path: no pre-existing outgoing metadata — attach extra directly
	// and skip a metadata.Join allocation. Safe because extra is captured
	// once in the interceptor closure and never mutated after Connect.
	existing, _ := metadata.FromOutgoingContext(ctx)
	if len(existing) == 0 {
		return metadata.NewOutgoingContext(ctx, extra)
	}
	return metadata.NewOutgoingContext(ctx, metadata.Join(existing, extra))
}

// Commitment is a convenience constructor for *CommitmentLevel.
func Commitment(c pb.CommitmentLevel) *pb.CommitmentLevel { return &c }

// DefaultKeepalive returns recommended keepalive parameters — ping every
// 10s, allow the server 1s to ack, keep pinging when no streams are open.
// Matches the defaults used by examples/golang and the Node client.
//
// A function rather than a package var so that callers cannot mutate
// shared state: returning a fresh value each call keeps this safe across
// goroutines.
func DefaultKeepalive() keepalive.ClientParameters {
	return keepalive.ClientParameters{
		Time:                10 * time.Second,
		Timeout:             time.Second,
		PermitWithoutStream: true,
	}
}
