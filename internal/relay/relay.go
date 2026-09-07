// Package relay is the gateway-side transport core: publish a request
// onto the INFERENCE work queue and relay the response frames back.
package relay

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/infbus/infbus/internal/wire"
)

type Request struct {
	Model, Org, Project, KeyID, Alias, ReqID, Kind string
	Deadline                                       time.Time
	Body                                           []byte
}

// RemoteError carries a worker-reported error frame.
type RemoteError struct{ Err wire.WireError }

func (e *RemoteError) Error() string { return fmt.Sprintf("remote %s: %s", e.Err.Code, e.Err.Message) }

func Publish(ctx context.Context, js jetstream.JetStream, r Request) (uint64, error) {
	msg := nats.NewMsg(wire.ReqSubject(r.Model))
	msg.Data = r.Body
	h := msg.Header
	h.Set(wire.HdrOrg, r.Org)
	h.Set(wire.HdrProject, r.Project)
	h.Set(wire.HdrKeyID, r.KeyID)
	h.Set(wire.HdrAlias, r.Alias)
	h.Set(wire.HdrReqID, r.ReqID)
	h.Set(wire.HdrKind, r.Kind)
	h.Set(wire.HdrReply, wire.RespSubject(r.ReqID))
	h.Set(wire.HdrDeadline, r.Deadline.UTC().Format(time.RFC3339))
	ack, err := js.PublishMsg(ctx, msg)
	if err != nil {
		return 0, err
	}
	return ack.Sequence, nil
}

type Listener struct {
	sub  *nats.Subscription
	ch   chan *nats.Msg
	next int
	done bool
}

// Listen subscribes to the response subject. Call BEFORE Publish so no
// frame can be lost between publish and subscribe.
func Listen(nc *nats.Conn, reqID string) (*Listener, error) {
	ch := make(chan *nats.Msg, 256)
	sub, err := nc.ChanSubscribe(wire.RespSubject(reqID), ch)
	if err != nil {
		return nil, err
	}
	return &Listener{sub: sub, ch: ch}, nil
}

func (l *Listener) Close() { _ = l.sub.Unsubscribe() }

// Next returns frames in order; io.EOF after done/result; RemoteError on error frames.
func (l *Listener) Next(ctx context.Context) (wire.Message, error) {
	if l.done {
		return wire.Message{}, io.EOF
	}
	for {
		select {
		case <-ctx.Done():
			return wire.Message{}, ctx.Err()
		case raw := <-l.ch:
			var m wire.Message
			if err := json.Unmarshal(raw.Data, &m); err != nil {
				return wire.Message{}, err
			}
			if m.Seq != l.next {
				// core NATS is ordered per publisher; a gap means frames
				// were dropped (slow consumer) — fail loudly, never reorder.
				return wire.Message{}, errors.New("response frame gap")
			}
			l.next++
			switch m.Kind {
			case wire.KindError:
				l.done = true
				if m.Error == nil {
					m.Error = &wire.WireError{Code: "unknown", Message: "worker error", HTTPStatus: 502}
				}
				return wire.Message{}, &RemoteError{Err: *m.Error}
			case wire.KindDone, wire.KindResult:
				l.done = true
				return m, nil
			default:
				return m, nil
			}
		}
	}
}

func Cancel(nc *nats.Conn, reqID string) error {
	return nc.Publish(wire.CancelSubject(reqID), nil)
}

// DeleteQueued removes a still-queued request (phase-1 cancel). Losing
// the race with a worker pickup is fine — phase-2 cancel covers it.
func DeleteQueued(ctx context.Context, js jetstream.JetStream, seq uint64) error {
	s, err := js.Stream(ctx, wire.StreamInference)
	if err != nil {
		return err
	}
	if err := s.DeleteMsg(ctx, seq); err != nil && !errors.Is(err, jetstream.ErrMsgNotFound) {
		return err
	}
	return nil
}
