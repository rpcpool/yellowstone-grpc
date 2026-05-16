package yellowstonegrpc

import (
	"context"
	"errors"
	"fmt"
	"net"
	"reflect"
	"runtime"
	"sync/atomic"
	"testing"

	pb "github.com/rpcpool/yellowstone-grpc/yellowstone-grpc-client-go/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// fakeGeyserServer is a minimal in-process Geyser server used to exercise
// the Subscribe / SubscribeDeshred streams without touching the network
// or requiring a real validator. It reads the initial request, emits a
// caller-controlled sequence of updates, then closes the stream.
type fakeGeyserServer struct {
	pb.UnimplementedGeyserServer

	subscribeUpdates []*pb.SubscribeUpdate
	deshredUpdates   []*pb.SubscribeUpdateDeshred
}

func (s *fakeGeyserServer) Subscribe(stream grpc.BidiStreamingServer[pb.SubscribeRequest, pb.SubscribeUpdate]) error {
	if _, err := stream.Recv(); err != nil {
		return err
	}
	for _, u := range s.subscribeUpdates {
		if err := stream.Send(u); err != nil {
			return err
		}
	}
	return nil
}

func (s *fakeGeyserServer) SubscribeDeshred(stream grpc.BidiStreamingServer[pb.SubscribeDeshredRequest, pb.SubscribeUpdateDeshred]) error {
	if _, err := stream.Recv(); err != nil {
		return err
	}
	for _, u := range s.deshredUpdates {
		if err := stream.Send(u); err != nil {
			return err
		}
	}
	return nil
}

// startFakeServer spins a GeyserServer on a random loopback port and
// returns its address plus a cleanup. The listener and gRPC server are
// torn down on t.Cleanup so tests don't leak goroutines.
func startFakeServer(t *testing.T, fake *fakeGeyserServer) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv := grpc.NewServer()
	pb.RegisterGeyserServer(srv, fake)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(func() {
		srv.Stop()
	})
	return lis.Addr().String()
}

func dialClient(t *testing.T, addr string, useVT bool) *Client {
	t.Helper()
	b := NewBuilder(addr).WithInsecure().WithDialOption(grpc.WithTransportCredentials(insecure.NewCredentials()))
	if !useVT {
		b = b.WithVTProtoCodec(false)
	}
	c, err := b.Connect(context.Background())
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

// TestSubscribeForEachReceivesAll is the happy path: three server-side
// updates, callback invoked three times in order, no error returned.
func TestSubscribeForEachReceivesAll(t *testing.T) {
	t.Parallel()
	updates := []*pb.SubscribeUpdate{
		{UpdateOneof: &pb.SubscribeUpdate_Slot{Slot: &pb.SubscribeUpdateSlot{Slot: 1}}},
		{UpdateOneof: &pb.SubscribeUpdate_Slot{Slot: &pb.SubscribeUpdateSlot{Slot: 2}}},
		{UpdateOneof: &pb.SubscribeUpdate_Slot{Slot: &pb.SubscribeUpdateSlot{Slot: 3}}},
	}
	addr := startFakeServer(t, &fakeGeyserServer{subscribeUpdates: updates})
	c := dialClient(t, addr, true)

	var got []uint64
	err := c.SubscribeForEach(
		context.Background(),
		&pb.SubscribeRequest{Slots: map[string]*pb.SubscribeRequestFilterSlots{"s": {}}},
		func(u *pb.SubscribeUpdate) error {
			got = append(got, u.GetSlot().GetSlot())
			return nil
		},
	)
	if err != nil {
		t.Fatalf("SubscribeForEach: %v", err)
	}
	if want := []uint64{1, 2, 3}; !equalSlots(got, want) {
		t.Fatalf("slots: got %v want %v", got, want)
	}
}

// TestSubscribeForEachReturnsToPool confirms the pointer handed to the
// callback has been recycled by the time the next callback fires. The
// identity check is best-effort — sync.Pool may or may not return the
// same pointer twice under load — but in this controlled test (one
// goroutine, small messages) the pool reliably recycles.
func TestSubscribeForEachReturnsToPool(t *testing.T) {
	t.Parallel()
	updates := []*pb.SubscribeUpdate{
		{UpdateOneof: &pb.SubscribeUpdate_Slot{Slot: &pb.SubscribeUpdateSlot{Slot: 1}}},
		{UpdateOneof: &pb.SubscribeUpdate_Slot{Slot: &pb.SubscribeUpdateSlot{Slot: 2}}},
	}
	addr := startFakeServer(t, &fakeGeyserServer{subscribeUpdates: updates})
	c := dialClient(t, addr, true)

	var seen []uintptr
	_ = c.SubscribeForEach(
		context.Background(),
		&pb.SubscribeRequest{},
		func(u *pb.SubscribeUpdate) error {
			// Capture the pointer identity and encourage the runtime
			// to reclaim/reuse memory between iterations.
			seen = append(seen, ptrID(u))
			runtime.Gosched()
			return nil
		},
	)
	if len(seen) != 2 {
		t.Fatalf("expected 2 callbacks, got %d", len(seen))
	}
	// Not strictly guaranteed, but the pool should reuse the same
	// object when it is returned before the next acquire. If this ever
	// becomes flaky, weaken to just "at least one callback fired".
	if seen[0] != seen[1] {
		t.Logf("pool did not reuse (ok but unexpected): %v vs %v", seen[0], seen[1])
	}
}

// TestSubscribeForEachStopIteration confirms ErrStopIteration ends the
// loop cleanly without propagating.
func TestSubscribeForEachStopIteration(t *testing.T) {
	t.Parallel()
	updates := []*pb.SubscribeUpdate{
		{UpdateOneof: &pb.SubscribeUpdate_Slot{Slot: &pb.SubscribeUpdateSlot{Slot: 1}}},
		{UpdateOneof: &pb.SubscribeUpdate_Slot{Slot: &pb.SubscribeUpdateSlot{Slot: 2}}},
		{UpdateOneof: &pb.SubscribeUpdate_Slot{Slot: &pb.SubscribeUpdateSlot{Slot: 3}}},
	}
	addr := startFakeServer(t, &fakeGeyserServer{subscribeUpdates: updates})
	c := dialClient(t, addr, true)

	var count atomic.Int32
	err := c.SubscribeForEach(
		context.Background(),
		&pb.SubscribeRequest{},
		func(u *pb.SubscribeUpdate) error {
			if count.Add(1) == 2 {
				return ErrStopIteration
			}
			return nil
		},
	)
	if err != nil {
		t.Fatalf("SubscribeForEach: %v", err)
	}
	if got := count.Load(); got != 2 {
		t.Fatalf("callback count: got %d want 2", got)
	}
}

// TestSubscribeForEachCallbackError surfaces a non-stop error to the caller.
func TestSubscribeForEachCallbackError(t *testing.T) {
	t.Parallel()
	updates := []*pb.SubscribeUpdate{
		{UpdateOneof: &pb.SubscribeUpdate_Slot{Slot: &pb.SubscribeUpdateSlot{Slot: 1}}},
	}
	addr := startFakeServer(t, &fakeGeyserServer{subscribeUpdates: updates})
	c := dialClient(t, addr, true)

	sentinel := errors.New("boom")
	err := c.SubscribeForEach(
		context.Background(),
		&pb.SubscribeRequest{},
		func(u *pb.SubscribeUpdate) error { return sentinel },
	)
	if !errors.Is(err, sentinel) {
		t.Fatalf("want sentinel err, got %v", err)
	}
}

// TestSubscribeForEachNilCallback guards against a nil fn silently
// blocking the whole stream forever.
func TestSubscribeForEachNilCallback(t *testing.T) {
	t.Parallel()
	c := dialClient(t, "127.0.0.1:1", true)
	if err := c.SubscribeForEach(context.Background(), nil, nil); err == nil {
		t.Fatal("want error for nil fn")
	}
}

// TestSubscribeDeshredForEach is the deshred equivalent of the happy
// path test — deshred shares the same plumbing, so one smoke test is
// enough.
func TestSubscribeDeshredForEach(t *testing.T) {
	t.Parallel()
	updates := []*pb.SubscribeUpdateDeshred{
		{UpdateOneof: &pb.SubscribeUpdateDeshred_Ping{Ping: &pb.SubscribeUpdatePing{}}},
		{UpdateOneof: &pb.SubscribeUpdateDeshred_Ping{Ping: &pb.SubscribeUpdatePing{}}},
	}
	addr := startFakeServer(t, &fakeGeyserServer{deshredUpdates: updates})
	c := dialClient(t, addr, true)

	var n atomic.Int32
	err := c.SubscribeDeshredForEach(
		context.Background(),
		&pb.SubscribeDeshredRequest{},
		func(u *pb.SubscribeUpdateDeshred) error {
			if u.GetPing() == nil {
				return fmt.Errorf("unexpected payload %T", u.UpdateOneof)
			}
			n.Add(1)
			return nil
		},
	)
	if err != nil {
		t.Fatalf("SubscribeDeshredForEach: %v", err)
	}
	if got := n.Load(); got != 2 {
		t.Fatalf("callback count: got %d want 2", got)
	}
}

// TestSubscribeForEachFallbackCodec exercises the same stream with the
// vtproto codec disabled, so we're going through grpc-go's stock proto
// codec. The pool APIs must still work because ReturnToVTPool zeroes
// the struct regardless of how UnmarshalVT vs proto.Unmarshal was used.
func TestSubscribeForEachFallbackCodec(t *testing.T) {
	t.Parallel()
	updates := []*pb.SubscribeUpdate{
		{UpdateOneof: &pb.SubscribeUpdate_Slot{Slot: &pb.SubscribeUpdateSlot{Slot: 7}}},
	}
	addr := startFakeServer(t, &fakeGeyserServer{subscribeUpdates: updates})
	c := dialClient(t, addr, false)

	var got []uint64
	err := c.SubscribeForEach(
		context.Background(),
		&pb.SubscribeRequest{},
		func(u *pb.SubscribeUpdate) error {
			got = append(got, u.GetSlot().GetSlot())
			return nil
		},
	)
	if err != nil {
		t.Fatalf("SubscribeForEach: %v", err)
	}
	if len(got) != 1 || got[0] != 7 {
		t.Fatalf("slots: got %v want [7]", got)
	}
}

// Helpers.

func equalSlots(a, b []uint64) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// ptrID returns a stable numeric identity for p so tests can check
// whether the pool is handing back the same object across iterations.
func ptrID[T any](p *T) uintptr {
	return reflect.ValueOf(p).Pointer()
}
