package relay_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/infbus/infbus/internal/relay"
	"github.com/infbus/infbus/internal/testutil"
	"github.com/infbus/infbus/internal/wire"
)

func TestPublishSetsHeadersAndReturnsSeq(t *testing.T) {
	nc, js := testutil.RunNATS(t)
	ctx := context.Background()
	if err := wire.EnsureStreams(ctx, js); err != nil {
		t.Fatal(err)
	}
	seq, err := relay.Publish(ctx, js, relay.Request{
		Model: "m1", Org: "acme", Project: "prod", KeyID: "k1", Alias: "fast",
		ReqID: "r1", Kind: "chat", Deadline: time.Now().Add(time.Minute),
		Body: []byte(`{"model":"fast"}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	if seq == 0 {
		t.Fatal("seq must be the JetStream stream sequence")
	}
	// read the message straight off the stream and check headers
	s, _ := js.Stream(ctx, wire.StreamInference)
	raw, err := s.GetMsg(ctx, seq)
	if err != nil {
		t.Fatal(err)
	}
	if got := raw.Header.Get(wire.HdrReply); got != wire.RespSubject("r1") {
		t.Fatalf("reply header = %q", got)
	}
	if got := raw.Header.Get(wire.HdrOrg); got != "acme" {
		t.Fatalf("org header = %q", got)
	}
	_ = nc
}

func TestListenerOrdersAndTerminates(t *testing.T) {
	nc, _ := testutil.RunNATS(t)
	l, err := relay.Listen(nc, "r2")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	pub := func(m wire.Message) {
		b, _ := json.Marshal(m)
		if err := nc.Publish(wire.RespSubject("r2"), b); err != nil {
			t.Fatal(err)
		}
	}
	pub(wire.Message{Kind: wire.KindChunk, Seq: 0, Payload: json.RawMessage(`{"a":1}`)})
	pub(wire.Message{Kind: wire.KindChunk, Seq: 1, Payload: json.RawMessage(`{"a":2}`)})
	pub(wire.Message{Kind: wire.KindDone, Seq: 2, Usage: &wire.Usage{PromptTokens: 1}})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var kinds []wire.Kind
	for {
		m, err := l.Next(ctx)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		kinds = append(kinds, m.Kind)
	}
	if len(kinds) != 3 || kinds[2] != wire.KindDone {
		t.Fatalf("kinds = %v", kinds)
	}
}

func TestListenerSurfacesRemoteError(t *testing.T) {
	nc, _ := testutil.RunNATS(t)
	l, err := relay.Listen(nc, "r3")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	b, _ := json.Marshal(wire.Message{Kind: wire.KindError, Seq: 0,
		Error: &wire.WireError{Code: "upstream_error", Message: "boom", HTTPStatus: 502}})
	if err := nc.Publish(wire.RespSubject("r3"), b); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err = l.Next(ctx)
	var re *relay.RemoteError
	if !errors.As(err, &re) || re.Err.HTTPStatus != 502 {
		t.Fatalf("err = %v", err)
	}
	_ = nats.ErrTimeout
}
