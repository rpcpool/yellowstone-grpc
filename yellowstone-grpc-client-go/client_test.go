package yellowstonegrpc

import (
	"context"
	"errors"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

func TestResolveEndpoint(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name           string
		endpoint       string
		forcePlaintext bool
		wantAddr       string
		wantPlaintext  bool
		wantErr        bool
	}{
		{name: "https full url", endpoint: "https://api.rpcpool.com:443", wantAddr: "api.rpcpool.com:443"},
		{name: "https infer port", endpoint: "https://api.rpcpool.com", wantAddr: "api.rpcpool.com:443"},
		{name: "http full url", endpoint: "http://localhost:10000", wantAddr: "localhost:10000", wantPlaintext: true},
		{name: "http infer port", endpoint: "http://localhost", wantAddr: "localhost:80", wantPlaintext: true},
		{name: "bare host:port (would break with naive url.Parse)", endpoint: "localhost:10000", wantAddr: "localhost:10000"},
		{name: "bare host:port with dots", endpoint: "api.rpcpool.com:443", wantAddr: "api.rpcpool.com:443"},
		{name: "forcePlaintext overrides https", endpoint: "https://api.rpcpool.com:443", forcePlaintext: true, wantAddr: "api.rpcpool.com:443", wantPlaintext: true},
		{name: "forcePlaintext applies to bare address", endpoint: "localhost:10000", forcePlaintext: true, wantAddr: "localhost:10000", wantPlaintext: true},
		{name: "https missing hostname", endpoint: "https://", wantErr: true},
		{name: "empty endpoint", endpoint: "", wantErr: true},
		{name: "grpc scheme rejected", endpoint: "grpc://host:1234", wantErr: true},
		{name: "tcp scheme rejected", endpoint: "tcp://host:1234", wantErr: true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			addr, plaintext, err := resolveEndpoint(tc.endpoint, tc.forcePlaintext)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("want error, got addr=%q plaintext=%v", addr, plaintext)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if addr != tc.wantAddr {
				t.Errorf("addr: got %q want %q", addr, tc.wantAddr)
			}
			if plaintext != tc.wantPlaintext {
				t.Errorf("plaintext: got %v want %v", plaintext, tc.wantPlaintext)
			}
		})
	}
}

// The tests below are ports of the Rust client tests in
// yellowstone-grpc-client/src/lib.rs. They exercise the Builder -> Connect
// lazy path (grpc.NewClient does not dial until the first RPC), so they
// run offline and are safe for CI.

func TestChannelHTTPSSuccess(t *testing.T) {
	t.Parallel()
	c, err := NewBuilder("https://ams17.rpcpool.com:443").
		WithXToken("1000000000000000000000000007").
		Connect(context.Background())
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer c.Close()
}

func TestChannelHTTPSuccess(t *testing.T) {
	t.Parallel()
	c, err := NewBuilder("http://127.0.0.1:10000").
		WithXToken("1234567891012141618202224268").
		Connect(context.Background())
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer c.Close()
}

func TestChannelEmptyTokenAccepted(t *testing.T) {
	t.Parallel()
	c, err := NewBuilder("http://127.0.0.1:10000").
		WithXToken("").
		Connect(context.Background())
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer c.Close()
}

func TestChannelNoToken(t *testing.T) {
	t.Parallel()
	c, err := NewBuilder("http://127.0.0.1:10000").
		Connect(context.Background())
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer c.Close()
}

// TestChannelInvalidURI mirrors the Rust test's intent: malformed URIs
// should fail fast at build time. The Rust client validates via tonic's
// URI parser and rejects paths like "sites/files/images/picture.png". Go's
// grpc.NewClient is more permissive, so the equivalent check happens in
// resolveEndpoint — which rejects http(s) URLs missing a hostname.
func TestChannelInvalidURI(t *testing.T) {
	t.Parallel()
	if _, err := NewBuilder("https://").Connect(context.Background()); err == nil {
		t.Fatalf("expected error for URL with no hostname")
	}
}

func TestConnectUDSEmptyPath(t *testing.T) {
	t.Parallel()
	if _, err := NewBuilder("ignored").ConnectUDS(context.Background(), ""); err == nil {
		t.Fatalf("expected error for empty UDS path")
	}
}

// TestWaitReadyCancelled verifies that WaitReady honours ctx cancellation.
// Using port 1 as an intentionally unreachable target means the underlying
// connection never reaches Ready; an already-cancelled ctx should cause
// WaitReady to return context.Canceled quickly.
func TestWaitReadyCancelled(t *testing.T) {
	t.Parallel()
	c, err := NewBuilder("http://127.0.0.1:1").Connect(context.Background())
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer c.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	waitErr := c.WaitReady(ctx)
	if !errors.Is(waitErr, context.Canceled) && !errors.Is(waitErr, context.DeadlineExceeded) {
		t.Fatalf("WaitReady: want ctx cancellation error, got %v", waitErr)
	}
}

func TestMetadataUnaryInterceptorInjectsHeader(t *testing.T) {
	t.Parallel()
	base := metadata.New(map[string]string{"x-token": "tk"})
	var gotCtx context.Context
	invoker := func(ctx context.Context, _ string, _, _ any, _ *grpc.ClientConn, _ ...grpc.CallOption) error {
		gotCtx = ctx
		return nil
	}
	if err := metadataUnaryInterceptor(base)(context.Background(), "/m", nil, nil, nil, invoker); err != nil {
		t.Fatalf("interceptor: %v", err)
	}
	md, ok := metadata.FromOutgoingContext(gotCtx)
	if !ok {
		t.Fatal("no outgoing metadata attached")
	}
	if got := md.Get("x-token"); len(got) != 1 || got[0] != "tk" {
		t.Errorf("x-token: got %v", got)
	}
}

func TestMetadataStreamInterceptorInjectsHeader(t *testing.T) {
	t.Parallel()
	base := metadata.New(map[string]string{"x-token": "tk"})
	var gotCtx context.Context
	streamer := func(ctx context.Context, _ *grpc.StreamDesc, _ *grpc.ClientConn, _ string, _ ...grpc.CallOption) (grpc.ClientStream, error) {
		gotCtx = ctx
		return nil, nil
	}
	if _, err := metadataStreamInterceptor(base)(context.Background(), nil, nil, "/m", streamer); err != nil {
		t.Fatalf("interceptor: %v", err)
	}
	md, ok := metadata.FromOutgoingContext(gotCtx)
	if !ok {
		t.Fatal("no outgoing metadata attached")
	}
	if got := md.Get("x-token"); len(got) != 1 || got[0] != "tk" {
		t.Errorf("x-token: got %v", got)
	}
}

// TestMetadataInterceptorPreservesExisting ensures that when the caller
// already has outgoing metadata attached to ctx, the interceptor merges
// rather than replaces it — protecting the optimization in mergeMetadata.
func TestMetadataInterceptorPreservesExisting(t *testing.T) {
	t.Parallel()
	base := metadata.New(map[string]string{"x-token": "tk"})
	existing := metadata.New(map[string]string{"foo": "bar"})
	ctx := metadata.NewOutgoingContext(context.Background(), existing)

	var gotCtx context.Context
	invoker := func(ctx context.Context, _ string, _, _ any, _ *grpc.ClientConn, _ ...grpc.CallOption) error {
		gotCtx = ctx
		return nil
	}
	if err := metadataUnaryInterceptor(base)(ctx, "/m", nil, nil, nil, invoker); err != nil {
		t.Fatalf("interceptor: %v", err)
	}
	md, _ := metadata.FromOutgoingContext(gotCtx)
	if got := md.Get("x-token"); len(got) != 1 || got[0] != "tk" {
		t.Errorf("x-token: got %v", got)
	}
	if got := md.Get("foo"); len(got) != 1 || got[0] != "bar" {
		t.Errorf("foo: got %v", got)
	}
}

func TestDefaultKeepaliveIsolated(t *testing.T) {
	t.Parallel()
	kp := DefaultKeepalive()
	if kp.Time != 10*time.Second {
		t.Errorf("Time: got %v, want 10s", kp.Time)
	}
	// Mutating the returned value must not affect a fresh call.
	kp.Time = 1 * time.Hour
	if DefaultKeepalive().Time != 10*time.Second {
		t.Errorf("DefaultKeepalive() leaked caller mutation")
	}
}

func FuzzResolveEndpoint(f *testing.F) {
	seeds := []struct {
		ep    string
		plain bool
	}{
		{"https://api.rpcpool.com:443", false},
		{"http://localhost:10000", true},
		{"localhost:10000", false},
		{"", false},
		{"grpc://host:1", false},
		{"https://", false},
		{"://bogus", false},
	}
	for _, s := range seeds {
		f.Add(s.ep, s.plain)
	}
	f.Fuzz(func(t *testing.T, endpoint string, forcePlaintext bool) {
		_, _, _ = resolveEndpoint(endpoint, forcePlaintext)
	})
}
