package yellowstonegrpc

import (
	"context"
	"errors"
	"fmt"
	"io"

	pb "github.com/rpcpool/yellowstone-grpc/yellowstone-grpc-client-go/proto"
	"google.golang.org/grpc"
)

// ErrStopIteration, returned from a ForEach callback, ends iteration
// cleanly without being surfaced as an error to the caller. Any other
// non-nil return from the callback is wrapped and returned.
var ErrStopIteration = errors.New("yellowstonegrpc: stop iteration")

// SubscribeForEach opens the Subscribe stream, sends req, and invokes
// fn for every update received. The *SubscribeUpdate passed to fn is
// drawn from a sync.Pool and returned to it as soon as fn returns —
// callers MUST NOT retain the pointer past the callback. If you need
// the value later, copy the fields you care about (u.CloneVT() returns
// a deep, non-pooled copy).
//
// fn may return ErrStopIteration to end iteration cleanly; any other
// error is returned to the caller unchanged. An io.EOF from the server
// also ends iteration cleanly and returns nil.
//
// This API exists alongside SubscribeOnce (which returns a stream the
// caller drives with Recv) so existing code stays source-compatible —
// pooling is opt-in.
func (c *Client) SubscribeForEach(
	ctx context.Context,
	req *pb.SubscribeRequest,
	fn func(*pb.SubscribeUpdate) error,
	opts ...grpc.CallOption,
) error {
	if fn == nil {
		return errors.New("SubscribeForEach: fn is required")
	}
	stream, err := c.geyser.Subscribe(ctx, opts...)
	if err != nil {
		return fmt.Errorf("open subscribe stream: %w", err)
	}
	if req != nil {
		if err := stream.Send(req); err != nil {
			_ = stream.CloseSend()
			return fmt.Errorf("send initial subscribe request: %w", err)
		}
	}
	for {
		u := pb.SubscribeUpdateFromVTPool()
		if err := stream.RecvMsg(u); err != nil {
			u.ReturnToVTPool()
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}
		cbErr := fn(u)
		u.ReturnToVTPool()
		if cbErr != nil {
			if errors.Is(cbErr, ErrStopIteration) {
				return nil
			}
			return cbErr
		}
	}
}

// SubscribeDeshredForEach is the deshred counterpart of SubscribeForEach.
// Same borrow semantics: the *SubscribeUpdateDeshred passed to fn is
// pool-owned and invalidated when fn returns.
func (c *Client) SubscribeDeshredForEach(
	ctx context.Context,
	req *pb.SubscribeDeshredRequest,
	fn func(*pb.SubscribeUpdateDeshred) error,
	opts ...grpc.CallOption,
) error {
	if fn == nil {
		return errors.New("SubscribeDeshredForEach: fn is required")
	}
	stream, err := c.geyser.SubscribeDeshred(ctx, opts...)
	if err != nil {
		return fmt.Errorf("open deshred stream: %w", err)
	}
	if req != nil {
		if err := stream.Send(req); err != nil {
			_ = stream.CloseSend()
			return fmt.Errorf("send initial deshred request: %w", err)
		}
	}
	for {
		u := pb.SubscribeUpdateDeshredFromVTPool()
		if err := stream.RecvMsg(u); err != nil {
			u.ReturnToVTPool()
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}
		cbErr := fn(u)
		u.ReturnToVTPool()
		if cbErr != nil {
			if errors.Is(cbErr, ErrStopIteration) {
				return nil
			}
			return cbErr
		}
	}
}
