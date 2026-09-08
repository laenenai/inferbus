package worker_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	ibengine "github.com/laenenai/inferbus/internal/engine"
	"github.com/laenenai/inferbus/internal/relay"
	"github.com/laenenai/inferbus/internal/testutil"
	"github.com/laenenai/inferbus/internal/wire"
)

// TestWorkerEmbedRequest: publish a request with header Ib-Kind: embed to a
// worker whose FakeEngine returns a known embeddings body; assert exactly
// one frame, Kind == wire.KindResult, payload == the engine's body, and the
// METERING usage event has Kind == "embed" with the fake's prompt tokens.
func TestWorkerEmbedRequest(t *testing.T) {
	embedResp := json.RawMessage(`{"object":"list","data":[{"object":"embedding","embedding":[0.5,0.6],"index":0}]}`)
	eng := &testutil.FakeEngine{
		EmbedResponse: embedResp,
		EmbedUsage:    wire.Usage{PromptTokens: 7},
	}
	nc, js := startWorker(t, eng)

	sub, err := nc.SubscribeSync("metering.usage.>")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sub.Unsubscribe() })

	l, err := relay.Listen(nc, "req-embed")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(l.Close)
	_, err = relay.Publish(context.Background(), js, relay.Request{
		Model: "m1", Org: "acme", Project: "prod", KeyID: "k1", Alias: "fast",
		ReqID: "req-embed", Kind: "embed", Deadline: time.Now().Add(time.Minute),
		Body: []byte(`{"model":"fast","input":"hello world"}`),
	})
	if err != nil {
		t.Fatal(err)
	}

	msgs, err := drain(t, l)
	if err != nil {
		t.Fatal(err)
	}
	if len(msgs) != 1 || msgs[0].Kind != wire.KindResult {
		t.Fatalf("frames = %+v", msgs)
	}
	if string(msgs[0].Payload) != string(embedResp) {
		t.Fatalf("payload = %s, want %s", msgs[0].Payload, embedResp)
	}

	raw, err := sub.NextMsg(10 * time.Second)
	if err != nil {
		t.Fatal("no usage event for embed request")
	}
	var ev wire.UsageEvent
	if err := json.Unmarshal(raw.Data, &ev); err != nil {
		t.Fatal(err)
	}
	if ev.Kind != "embed" || ev.PromptTokens != 7 {
		t.Fatalf("usage event = %+v", ev)
	}
}

// TestWorkerEmbedUnsupported: FakeEngine.Embed returns engine.ErrUnsupported
// -> one KindError frame with code "unsupported_kind" and HTTPStatus 400;
// message acked (never redelivered); usage event status "error".
func TestWorkerEmbedUnsupported(t *testing.T) {
	eng := &testutil.FakeEngine{EmbedErr: ibengine.ErrUnsupported}
	nc, js := startWorker(t, eng)

	sub, err := nc.SubscribeSync("metering.usage.>")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sub.Unsubscribe() })

	l, err := relay.Listen(nc, "req-embed-unsupported")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(l.Close)
	_, err = relay.Publish(context.Background(), js, relay.Request{
		Model: "m1", Org: "acme", Project: "prod", KeyID: "k1", Alias: "fast",
		ReqID: "req-embed-unsupported", Kind: "embed", Deadline: time.Now().Add(time.Minute),
		Body: []byte(`{"model":"fast","input":"hello world"}`),
	})
	if err != nil {
		t.Fatal(err)
	}

	_, drainErr := drain(t, l)
	var re *relay.RemoteError
	if !errors.As(drainErr, &re) || re.Err.Code != "unsupported_kind" || re.Err.HTTPStatus != 400 {
		t.Fatalf("err = %v", drainErr)
	}

	raw, err := sub.NextMsg(10 * time.Second)
	if err != nil {
		t.Fatal("no usage event for unsupported embed request")
	}
	var ev wire.UsageEvent
	if err := json.Unmarshal(raw.Data, &ev); err != nil {
		t.Fatal(err)
	}
	if ev.Status != "error" {
		t.Fatalf("usage event = %+v", ev)
	}

	// Redelivery check: the message must have been acked, so JetStream
	// must not deliver it again. Since publishAndListen/relay.Listen use a
	// fresh core-NATS subject for responses, redelivery would show up as a
	// second KindError frame arriving on the same reply subject. Give it a
	// beat and confirm nothing more shows up.
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	if _, err := l.Next(ctx); err == nil {
		t.Fatal("unexpected second frame: message was redelivered")
	}
}

// TestWorkerChatUnaffected: an Ib-Kind: chat (and an absent-header) request
// still goes through Chat -- proves the default and no regression.
func TestWorkerChatUnaffected(t *testing.T) {
	for _, kind := range []string{"chat", ""} {
		kind := kind
		t.Run("kind="+kind, func(t *testing.T) {
			eng := &testutil.FakeEngine{FinalUsage: wire.Usage{PromptTokens: 2, CompletionTokens: 1}}
			nc, js := startWorker(t, eng)
			reqID := "req-chat-unaffected-" + kind
			if kind == "" {
				reqID = "req-chat-unaffected-empty"
			}
			l, err := relay.Listen(nc, reqID)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(l.Close)
			_, err = relay.Publish(context.Background(), js, relay.Request{
				Model: "m1", Org: "acme", Project: "prod", KeyID: "k1", Alias: "fast",
				ReqID: reqID, Kind: kind, Deadline: time.Now().Add(time.Minute),
				Body: []byte(`{"model":"fast","messages":[]}`), // no "stream"
			})
			if err != nil {
				t.Fatal(err)
			}
			msgs, err := drain(t, l)
			if err != nil {
				t.Fatal(err)
			}
			if len(msgs) != 1 || msgs[0].Kind != wire.KindResult {
				t.Fatalf("frames = %+v", msgs)
			}
			if msgs[0].Usage == nil || msgs[0].Usage.PromptTokens != 2 {
				t.Fatalf("result usage = %+v", msgs[0].Usage)
			}
		})
	}
}

// TestWorkerEmbedIgnoresStreamFlag: a body with "stream":true under
// Ib-Kind: embed still produces exactly one KindResult (the gateway rejects
// this case, but the worker must not try to stream an embedding).
func TestWorkerEmbedIgnoresStreamFlag(t *testing.T) {
	embedResp := json.RawMessage(`{"object":"list","data":[]}`)
	eng := &testutil.FakeEngine{EmbedResponse: embedResp, EmbedUsage: wire.Usage{PromptTokens: 3}}
	nc, js := startWorker(t, eng)

	l, err := relay.Listen(nc, "req-embed-stream-flag")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(l.Close)
	_, err = relay.Publish(context.Background(), js, relay.Request{
		Model: "m1", Org: "acme", Project: "prod", KeyID: "k1", Alias: "fast",
		ReqID: "req-embed-stream-flag", Kind: "embed", Deadline: time.Now().Add(time.Minute),
		Body: []byte(`{"model":"fast","input":"hello","stream":true}`),
	})
	if err != nil {
		t.Fatal(err)
	}

	msgs, err := drain(t, l)
	if err != nil {
		t.Fatal(err)
	}
	if len(msgs) != 1 || msgs[0].Kind != wire.KindResult {
		t.Fatalf("frames = %+v", msgs)
	}
	if string(msgs[0].Payload) != string(embedResp) {
		t.Fatalf("payload = %s, want %s", msgs[0].Payload, embedResp)
	}
}
