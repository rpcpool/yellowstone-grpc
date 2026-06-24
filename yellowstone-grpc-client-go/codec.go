package yellowstonegrpc

import (
	"fmt"

	"google.golang.org/grpc/encoding"
	// Importing the standard proto codec ensures it is registered in the
	// encoding registry — we never hit that registry ourselves, since we
	// install vtCodec per-connection via grpc.ForceCodecV2, but servers
	// (and any non-Yellowstone code in the user's process) still expect
	// the default to be available.
	_ "google.golang.org/grpc/encoding/proto"
	"google.golang.org/grpc/mem"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/protoadapt"
)

// codecName is the content-subtype advertised on the wire. We reuse
// "proto" so the server sees the same content-type it would for the
// standard codec; vtCodec is wired per-connection via grpc.ForceCodecV2
// and is never placed in the global encoding registry, so registering
// under this name cannot shadow the default for other callers.
const codecName = "proto"

// vtMarshaler / vtUnmarshaler are the subset of vtproto's generated
// interface that the codec needs. Messages produced by
// protoc-gen-go-vtproto implement both; messages without vtproto
// methods (e.g. grpc.health.v1 messages) fall through to proto.Marshal.
type vtMarshaler interface {
	SizeVT() int
	MarshalToSizedBufferVT([]byte) (int, error)
}

type vtUnmarshaler interface {
	UnmarshalVT([]byte) error
}

// vtCodec is a grpc/encoding.CodecV2 that uses vtproto's generated
// Marshal/Unmarshal methods when available, with a mem.BufferPool for
// wire buffers, and falls back to the standard google.golang.org/protobuf
// implementation otherwise. It is safe for concurrent use.
type vtCodec struct {
	pool mem.BufferPool
}

// newVTCodec returns a CodecV2 backed by grpc-go's default buffer pool,
// matching the tuning used by google.golang.org/grpc/encoding/proto.
func newVTCodec() *vtCodec {
	return &vtCodec{pool: mem.DefaultBufferPool()}
}

func (c *vtCodec) Name() string { return codecName }

func (c *vtCodec) Marshal(v any) (mem.BufferSlice, error) {
	if m, ok := v.(vtMarshaler); ok {
		return c.marshalVT(m)
	}
	pm, ok := toProtoMessage(v)
	if !ok {
		return nil, fmt.Errorf("yellowstonegrpc: cannot marshal %T (not a proto.Message)", v)
	}
	return c.marshalProto(pm)
}

func (c *vtCodec) marshalVT(m vtMarshaler) (mem.BufferSlice, error) {
	size := m.SizeVT()
	if mem.IsBelowBufferPoolingThreshold(size) {
		buf := make([]byte, size)
		if _, err := m.MarshalToSizedBufferVT(buf); err != nil {
			return nil, err
		}
		return mem.BufferSlice{mem.SliceBuffer(buf)}, nil
	}
	bufp := c.pool.Get(size)
	// MarshalToSizedBufferVT writes backwards starting from len(buf) and
	// expects the valid bytes to occupy [0:size] on return, so the slice
	// must be sized exactly. The pool may hand us a []byte with cap>size.
	*bufp = (*bufp)[:size]
	if _, err := m.MarshalToSizedBufferVT(*bufp); err != nil {
		c.pool.Put(bufp)
		return nil, err
	}
	return mem.BufferSlice{mem.NewBuffer(bufp, c.pool)}, nil
}

func (c *vtCodec) marshalProto(pm proto.Message) (mem.BufferSlice, error) {
	size := proto.Size(pm)
	if mem.IsBelowBufferPoolingThreshold(size) {
		buf, err := proto.Marshal(pm)
		if err != nil {
			return nil, err
		}
		return mem.BufferSlice{mem.SliceBuffer(buf)}, nil
	}
	bufp := c.pool.Get(size)
	out, err := (proto.MarshalOptions{}).MarshalAppend((*bufp)[:0], pm)
	if err != nil {
		c.pool.Put(bufp)
		return nil, err
	}
	*bufp = out
	return mem.BufferSlice{mem.NewBuffer(bufp, c.pool)}, nil
}

func (c *vtCodec) Unmarshal(data mem.BufferSlice, v any) error {
	buf := data.MaterializeToBuffer(c.pool)
	defer buf.Free()
	if u, ok := v.(vtUnmarshaler); ok {
		return u.UnmarshalVT(buf.ReadOnlyData())
	}
	pm, ok := toProtoMessage(v)
	if !ok {
		return fmt.Errorf("yellowstonegrpc: cannot unmarshal into %T (not a proto.Message)", v)
	}
	return proto.Unmarshal(buf.ReadOnlyData(), pm)
}

func toProtoMessage(v any) (proto.Message, bool) {
	switch vv := v.(type) {
	case protoadapt.MessageV2:
		return vv, true
	case protoadapt.MessageV1:
		return protoadapt.MessageV2Of(vv), true
	}
	return nil, false
}

// defaultVTCodec is the shared codec instance installed on every Client
// by default. Safe to share: the underlying mem.BufferPool is thread-safe
// and the codec holds no other mutable state. Callers wanting a distinct
// pool (e.g. for measurement) can opt out with Builder.WithVTProtoCodec(false).
var defaultVTCodec = newVTCodec()

// encodingCodecV2 is a compile-time assertion that vtCodec satisfies the
// interface gRPC expects — protects against silent breakage if the
// CodecV2 signature ever changes upstream.
var _ encoding.CodecV2 = (*vtCodec)(nil)
