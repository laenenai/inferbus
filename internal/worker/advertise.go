package worker

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/laenenai/inferbus/internal/wire"
)

// advertise best-effort publishes this worker's presence — its worker id
// and served models — into the MODELS KV bucket, re-Putting on a
// heartbeat interval so the entry's TTL never lapses while the worker is
// alive. It never returns an error and never cancels ctx: a broken or
// unreachable MODELS bucket (missing permissions, JetStream down, a
// pre-existing incompatible stream) only disables advertisement, it must
// never take the worker itself down. Callers run it in its own goroutine
// and rely on ctx cancellation to stop it.
func (w *Worker) advertise(ctx context.Context) {
	ttl := w.cfg.AdvertiseTTL
	if ttl == 0 {
		ttl = wire.ModelsTTL
	}
	kv, err := w.js.CreateOrUpdateKeyValue(ctx, jetstream.KeyValueConfig{
		Bucket: wire.BucketModels, TTL: ttl,
	})
	if err != nil {
		slog.Warn("worker: MODELS advertise disabled", "err", err)
		return
	}

	started := time.Now().UTC()
	every := w.cfg.AdvertiseEvery
	if every == 0 {
		every = wire.ModelsHeartbeat
	}
	t := time.NewTicker(every)
	defer t.Stop()

	for {
		ad := wire.WorkerAd{WorkerID: w.cfg.WorkerID, StartedAt: started, LastSeen: time.Now().UTC()}
		for _, mc := range w.cfg.Models {
			eng := mc.Engine
			if eng == "" {
				eng = "openai_http"
			}
			ad.Models = append(ad.Models, wire.WorkerAdModel{Name: mc.Name, Engine: eng, MaxInflight: mc.MaxInflight})
		}
		b, _ := json.Marshal(ad)
		if _, err := kv.Put(ctx, w.cfg.WorkerID, b); err != nil && ctx.Err() == nil {
			slog.Warn("worker: MODELS heartbeat failed", "err", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}
