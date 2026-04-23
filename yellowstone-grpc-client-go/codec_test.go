package yellowstonegrpc

import (
	"bytes"
	"testing"

	pb "github.com/rpcpool/yellowstone-grpc/yellowstone-grpc-client-go/proto"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/mem"
	"google.golang.org/protobuf/proto"
)

// TestVTCodecRoundtripVTMessage verifies the fast path — a message that
// implements vtproto's Marshal/Unmarshal interfaces roundtrips correctly.
// We use SubscribeUpdateSlot because it is small (below the buffer pool
// threshold) and exercises a non-empty nested message in the oneof.
func TestVTCodecRoundtripVTMessage(t *testing.T) {
	t.Parallel()
	c := newVTCodec()

	orig := &pb.SubscribeUpdate{
		Filters: []string{"slots"},
		UpdateOneof: &pb.SubscribeUpdate_Slot{
			Slot: &pb.SubscribeUpdateSlot{Slot: 42, Parent: ptr(uint64(41))},
		},
	}
	slice, err := c.Marshal(orig)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	defer slice.Free()

	got := &pb.SubscribeUpdate{}
	if err := c.Unmarshal(slice, got); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if !proto.Equal(orig, got) {
		t.Fatalf("roundtrip mismatch\n want %+v\n got  %+v", orig, got)
	}
}

// TestVTCodecRoundtripLargeMessage forces the pooled-buffer branch by
// producing a payload larger than mem.IsBelowBufferPoolingThreshold (1KiB).
func TestVTCodecRoundtripLargeMessage(t *testing.T) {
	t.Parallel()
	c := newVTCodec()

	// 4KiB of arbitrary data lives inside SubscribeUpdateAccountInfo.Data.
	big := make([]byte, 4096)
	for i := range big {
		big[i] = byte(i)
	}
	orig := &pb.SubscribeUpdate{
		UpdateOneof: &pb.SubscribeUpdate_Account{
			Account: &pb.SubscribeUpdateAccount{
				Account: &pb.SubscribeUpdateAccountInfo{
					Pubkey: bytes.Repeat([]byte{1}, 32),
					Owner:  bytes.Repeat([]byte{2}, 32),
					Data:   big,
				},
				Slot: 1,
			},
		},
	}
	slice, err := c.Marshal(orig)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	if slice.Len() <= 1024 {
		t.Fatalf("expected payload above pooling threshold, got %d bytes", slice.Len())
	}
	defer slice.Free()

	got := &pb.SubscribeUpdate{}
	if err := c.Unmarshal(slice, got); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if !proto.Equal(orig, got) {
		t.Fatalf("roundtrip mismatch for large message")
	}
}

// TestVTCodecFallbackNonVTMessage feeds the codec a proto.Message that
// does not implement vtproto's interfaces (grpc.health messages) and
// confirms it still roundtrips via the google.golang.org/protobuf path.
// Exercises the fallback that keeps health checks working.
func TestVTCodecFallbackNonVTMessage(t *testing.T) {
	t.Parallel()
	c := newVTCodec()

	orig := &healthpb.HealthCheckRequest{Service: "geyser.Geyser"}
	if _, ok := any(orig).(vtMarshaler); ok {
		t.Skip("healthpb unexpectedly implements vtproto; fallback test moot")
	}
	slice, err := c.Marshal(orig)
	if err != nil {
		t.Fatalf("Marshal fallback: %v", err)
	}
	defer slice.Free()

	got := &healthpb.HealthCheckRequest{}
	if err := c.Unmarshal(slice, got); err != nil {
		t.Fatalf("Unmarshal fallback: %v", err)
	}
	if got.Service != orig.Service {
		t.Errorf("roundtrip: got %q want %q", got.Service, orig.Service)
	}
}

// TestVTCodecName checks we advertise "proto" — same content-subtype
// as the default codec — so no server-side changes are needed.
func TestVTCodecName(t *testing.T) {
	t.Parallel()
	if got := newVTCodec().Name(); got != "proto" {
		t.Errorf("Name: got %q want %q", got, "proto")
	}
}

// TestVTCodecRejectsNonProto confirms the codec surfaces a clear error
// when asked to handle something that is neither vtproto- nor proto-
// compatible, instead of panicking deep inside grpc.
func TestVTCodecRejectsNonProto(t *testing.T) {
	t.Parallel()
	c := newVTCodec()
	if _, err := c.Marshal("not a proto message"); err == nil {
		t.Fatal("Marshal: want error for non-proto value")
	}
	if err := c.Unmarshal(mem.BufferSlice{mem.SliceBuffer([]byte{0})}, "not a proto message"); err == nil {
		t.Fatal("Unmarshal: want error for non-proto value")
	}
}

func ptr[T any](v T) *T { return &v }
