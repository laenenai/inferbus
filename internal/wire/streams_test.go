package wire_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/infbus/infbus/internal/testutil"
	"github.com/infbus/infbus/internal/wire"
)

func TestEnsureStreamsIdempotent(t *testing.T) {
	_, js := testutil.RunNATS(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for i := 0; i < 2; i++ { // second call must not error
		if err := wire.EnsureStreams(ctx, js); err != nil {
			t.Fatalf("EnsureStreams pass %d: %v", i+1, err)
		}
	}
	info, err := js.Stream(ctx, wire.StreamInference)
	if err != nil {
		t.Fatalf("stream lookup: %v", err)
	}
	cfg := info.CachedInfo().Config
	if len(cfg.Subjects) != 1 || cfg.Subjects[0] != "inference.req.>" {
		t.Fatalf("INFERENCE subjects = %v", cfg.Subjects)
	}
	if _, err := js.Stream(ctx, wire.StreamMetering); err != nil {
		t.Fatalf("METERING missing: %v", err)
	}
}

func TestMessageRoundTrip(t *testing.T) {
	in := wire.Message{Kind: wire.KindChunk, Seq: 3, Payload: json.RawMessage(`{"x":1}`)}
	b, _ := json.Marshal(in)
	var out wire.Message
	if err := json.Unmarshal(b, &out); err != nil {
		t.Fatal(err)
	}
	if out.Kind != wire.KindChunk || out.Seq != 3 || string(out.Payload) != `{"x":1}` {
		t.Fatalf("round trip mismatch: %+v", out)
	}
}
