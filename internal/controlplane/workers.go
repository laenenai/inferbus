// GET /admin/v1/workers (M5 hardening, Task 5): a fleet-wide view of every
// worker currently advertising itself in the MODELS KV bucket (wire.
// BucketModels, populated by internal/worker/advertise.go). This is a
// read-only, best-effort presence snapshot — entries expire via the
// bucket's own TTL when a worker stops heartbeating, so a worker that is
// merely slow to report is indistinguishable here from one that is gone.
package controlplane

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"sort"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/laenenai/inferbus/internal/wire"
)

// listWorkers is PlatformAdmin-only (same gate as resyncProjections and
// listOrgs: a system-wide view across every org's traffic, not scoped to
// any one org a caller might have a role in).
func (a *Admin) listWorkers(w http.ResponseWriter, r *http.Request) {
	id, ok := a.authenticate(w, r)
	if !ok {
		return
	}
	if !id.PlatformAdmin {
		writeAdminError(w, http.StatusForbidden, errTypePermission, "listing workers requires platform admin")
		return
	}

	workers, err := a.fetchWorkers(r.Context())
	if err != nil {
		writeInternalError(w, "listWorkers", err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"workers": workers})
}

// fetchWorkers reads every entry out of the MODELS KV bucket and returns
// them sorted by worker_id.
//
// A bucket that has never been created (jetstream.ErrBucketNotFound — no
// worker has ever started against this deployment) is not an error: it
// reports an empty fleet, same as a bucket that exists but is currently
// empty (jetstream.ErrNoKeysFound from ListKeys).
//
// A malformed entry (should never happen from advertise.go's own writer,
// but Admin must not let one bad KV value take the whole endpoint down) is
// skipped with a slog.Warn rather than failing the request; the rest of
// the fleet is still returned.
func (a *Admin) fetchWorkers(ctx context.Context) ([]wire.WorkerAd, error) {
	out := []wire.WorkerAd{}

	kv, err := a.js.KeyValue(ctx, wire.BucketModels)
	if err != nil {
		if errors.Is(err, jetstream.ErrBucketNotFound) {
			return out, nil
		}
		return nil, err
	}

	lister, err := kv.ListKeys(ctx)
	if err != nil {
		if errors.Is(err, jetstream.ErrNoKeysFound) {
			return out, nil
		}
		return nil, err
	}
	defer lister.Stop()

	for key := range lister.Keys() {
		entry, err := kv.Get(ctx, key)
		if err != nil {
			if errors.Is(err, jetstream.ErrKeyNotFound) {
				// Expired/deleted between ListKeys and Get (the bucket's
				// own TTL racing this read) — just skip it, not an error.
				continue
			}
			return nil, err
		}
		var ad wire.WorkerAd
		if err := json.Unmarshal(entry.Value(), &ad); err != nil {
			slog.Warn("controlplane: admin: skipping malformed MODELS KV entry", "key", key, "error", err)
			continue
		}
		out = append(out, ad)
	}

	sort.Slice(out, func(i, j int) bool { return out[i].WorkerID < out[j].WorkerID })
	return out, nil
}
