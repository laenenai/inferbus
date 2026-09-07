package controlplane_test

import (
	"context"
	"encoding/json"
	"errors"
	"sort"
	"testing"
	"time"

	controlplanev1 "github.com/laenenai/inferbus/api/controlplane/v1"
	"github.com/laenenai/inferbus/internal/controlplane"
	"github.com/laenenai/inferbus/internal/controlplane/alias"
	"github.com/laenenai/inferbus/internal/controlplane/apikey"
	"github.com/laenenai/inferbus/internal/testutil"

	"github.com/laenenai/es-lite/aggregate"
	"github.com/laenenai/es-lite/es"
	"github.com/laenenai/es-lite/sqlite"
	"github.com/nats-io/nats.go/jetstream"
)

// --- SplitAliasStreamID -----------------------------------------------

func TestSplitAliasStreamID(t *testing.T) {
	cases := []struct {
		name      string
		id        string
		wantScope string
		wantName  string
		wantErr   bool
	}{
		{name: "global", id: "g_fast", wantScope: "_global", wantName: "fast"},
		{name: "org", id: "o_acme_smart", wantScope: "acme", wantName: "smart"},
		{name: "org multi-char name unaffected", id: "o_acme_smart-2", wantScope: "acme", wantName: "smart-2"},
		{name: "missing separator", id: "fast", wantErr: true},
		{name: "empty", id: "", wantErr: true},
		{name: "unknown prefix", id: "x_fast", wantErr: true},
		{name: "global empty name", id: "g_", wantErr: true},
		{name: "org missing name separator", id: "o_acme", wantErr: true},
		{name: "org empty org id", id: "o__smart", wantErr: true},
		{name: "org empty name", id: "o_acme_", wantErr: true},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			scope, name, err := controlplane.SplitAliasStreamID(c.id)
			if c.wantErr {
				if err == nil {
					t.Fatalf("SplitAliasStreamID(%q) = (%q, %q, nil), want error", c.id, scope, name)
				}
				return
			}
			if err != nil {
				t.Fatalf("SplitAliasStreamID(%q) unexpected error: %v", c.id, err)
			}
			if scope != c.wantScope || name != c.wantName {
				t.Fatalf("SplitAliasStreamID(%q) = (%q, %q), want (%q, %q)", c.id, scope, name, c.wantScope, c.wantName)
			}
		})
	}
}

// --- shared test scaffolding --------------------------------------------

// kvProjTestEnv wires a sqlite store, embedded NATS/JetStream, the
// CONTROL_EVENTS stream, and a running relay, all torn down on test
// cleanup. It mirrors relay_test.go's setup so aggregate commands appended
// via the returned store actually reach the projectors under test.
type kvProjTestEnv struct {
	store es.Store
	js    jetstream.JetStream
}

func setupKVProjTest(t *testing.T, relayCtx context.Context) kvProjTestEnv {
	t.Helper()

	dsn := "file:" + t.Name() + "?mode=memory&cache=shared"
	store, err := sqlite.Open(context.Background(), dsn)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { store.Close() })

	_, js := testutil.RunNATS(t)

	if err := controlplane.EnsureControlStream(relayCtx, js); err != nil {
		t.Fatalf("ensure control stream: %v", err)
	}

	relayDone := make(chan error, 1)
	go func() {
		relayDone <- controlplane.RunRelay(relayCtx, store, js)
	}()
	t.Cleanup(func() {
		select {
		case <-relayDone:
		case <-time.After(5 * time.Second):
			t.Fatal("relay did not stop")
		}
	})

	return kvProjTestEnv{store: store, js: js}
}

// waitForKV polls until check returns true or the timeout elapses.
func waitForKV(t *testing.T, check func() (bool, error)) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	var lastErr error
	for time.Now().Before(deadline) {
		ok, err := check()
		if err != nil {
			lastErr = err
		} else if ok {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("condition not met before timeout (last error: %v)", lastErr)
}

func getAliasEntry(t *testing.T, kv jetstream.KeyValue, key string) (controlplane.AliasEntry, bool) {
	t.Helper()
	entry, err := kv.Get(context.Background(), key)
	if err != nil {
		if errors.Is(err, jetstream.ErrKeyNotFound) {
			return controlplane.AliasEntry{}, false
		}
		t.Fatalf("get %q: %v", key, err)
	}
	var v controlplane.AliasEntry
	if err := json.Unmarshal(entry.Value(), &v); err != nil {
		t.Fatalf("unmarshal %q: %v", key, err)
	}
	return v, true
}

func getKeyEntry(t *testing.T, kv jetstream.KeyValue, hash string) (controlplane.KeyEntry, bool) {
	t.Helper()
	entry, err := kv.Get(context.Background(), hash)
	if err != nil {
		if errors.Is(err, jetstream.ErrKeyNotFound) {
			return controlplane.KeyEntry{}, false
		}
		t.Fatalf("get %q: %v", hash, err)
	}
	var v controlplane.KeyEntry
	if err := json.Unmarshal(entry.Value(), &v); err != nil {
		t.Fatalf("unmarshal %q: %v", hash, err)
	}
	return v, true
}

// waitForBucket polls until RunKVProjectors has created bucket (it does so
// lazily on start), then returns a bound handle to it.
func waitForBucket(t *testing.T, js jetstream.JetStream, bucket string) jetstream.KeyValue {
	t.Helper()
	var kv jetstream.KeyValue
	waitForKV(t, func() (bool, error) {
		var err error
		kv, err = js.KeyValue(context.Background(), bucket)
		if err != nil {
			if errors.Is(err, jetstream.ErrBucketNotFound) {
				return false, nil
			}
			return false, err
		}
		return true, nil
	})
	return kv
}

func bucketKeys(t *testing.T, kv jetstream.KeyValue) []string {
	t.Helper()
	keys, err := kv.Keys(context.Background())
	if err != nil {
		if errors.Is(err, jetstream.ErrNoKeysFound) {
			return nil
		}
		t.Fatalf("keys: %v", err)
	}
	sort.Strings(keys)
	return keys
}

// --- alias lifecycle ------------------------------------------------------

func TestKVProjectors_AliasLifecycle(t *testing.T) {
	relayCtx, cancelRelay := context.WithCancel(context.Background())
	defer cancelRelay()
	env := setupKVProjTest(t, relayCtx)

	rt := aggregate.NewRuntime(env.store, alias.Decider, alias.Codec())

	globalFast, err := es.NewStreamID(alias.StreamType, "g_fast")
	if err != nil {
		t.Fatalf("stream id: %v", err)
	}
	acmeSmart, err := es.NewStreamID(alias.StreamType, "o_acme_smart")
	if err != nil {
		t.Fatalf("stream id: %v", err)
	}

	if _, err := rt.Handle(context.Background(), globalFast, &controlplanev1.AliasCommand{
		Kind: &controlplanev1.AliasCommand_Set{Set: &controlplanev1.SetAlias{Target: "llama"}},
	}, es.Meta{}); err != nil {
		t.Fatalf("set g_fast: %v", err)
	}
	if _, err := rt.Handle(context.Background(), acmeSmart, &controlplanev1.AliasCommand{
		Kind: &controlplanev1.AliasCommand_Set{Set: &controlplanev1.SetAlias{
			Target: "claude",
			Params: map[string]string{"temperature": "0-2"},
		}},
	}, es.Meta{}); err != nil {
		t.Fatalf("set o_acme_smart: %v", err)
	}
	if _, err := rt.Handle(context.Background(), globalFast, &controlplanev1.AliasCommand{
		Kind: &controlplanev1.AliasCommand_Delete{Delete: &controlplanev1.DeleteAlias{}},
	}, es.Meta{}); err != nil {
		t.Fatalf("delete g_fast: %v", err)
	}

	projCtx, cancelProj := context.WithCancel(context.Background())
	defer cancelProj()
	projDone := make(chan error, 1)
	go func() { projDone <- controlplane.RunKVProjectors(projCtx, env.store, env.js) }()
	t.Cleanup(func() {
		cancelProj()
		select {
		case <-projDone:
		case <-time.After(5 * time.Second):
			t.Fatal("RunKVProjectors did not stop")
		}
	})

	aliasesKV := waitForBucket(t, env.js, controlplane.BucketAliases)

	waitForKV(t, func() (bool, error) {
		keys := bucketKeys(t, aliasesKV)
		return len(keys) == 1 && keys[0] == "acme/smart", nil
	})

	if keys := bucketKeys(t, aliasesKV); len(keys) != 1 || keys[0] != "acme/smart" {
		t.Fatalf("ALIASES bucket keys = %v, want exactly [acme/smart]", keys)
	}
	entry, ok := getAliasEntry(t, aliasesKV, "acme/smart")
	if !ok {
		t.Fatal("acme/smart entry missing")
	}
	if entry.Target != "claude" || entry.Params["temperature"] != "0-2" {
		t.Fatalf("acme/smart entry = %+v, want Target=claude Params[temperature]=0-2", entry)
	}
	if _, ok := getAliasEntry(t, aliasesKV, "_global/fast"); ok {
		t.Fatal("_global/fast entry still present, want deleted")
	}
}

// --- key lifecycle ----------------------------------------------------

func TestKVProjectors_KeyLifecycle(t *testing.T) {
	relayCtx, cancelRelay := context.WithCancel(context.Background())
	defer cancelRelay()
	env := setupKVProjTest(t, relayCtx)

	rt := aggregate.NewRuntime(env.store, apikey.Decider, apikey.Codec())
	stream, err := es.NewStreamID(apikey.StreamType, "key-1")
	if err != nil {
		t.Fatalf("stream id: %v", err)
	}

	const hash1 = "1111111111111111111111111111111111111111111111111111111111111111"
	const hash2 = "2222222222222222222222222222222222222222222222222222222222222222"

	if _, err := rt.Handle(context.Background(), stream, &controlplanev1.ApiKeyCommand{
		Kind: &controlplanev1.ApiKeyCommand_Create{Create: &controlplanev1.CreateKey{
			Id:           "key-1",
			Org:          "acme",
			Project:      "proj-1",
			Name:         "prod",
			Hash:         hash1,
			Allow:        []string{"gpt-4"},
			RateLimitRpm: 60,
		}},
	}, es.Meta{}); err != nil {
		t.Fatalf("create: %v", err)
	}

	projCtx, cancelProj := context.WithCancel(context.Background())
	defer cancelProj()
	projDone := make(chan error, 1)
	go func() { projDone <- controlplane.RunKVProjectors(projCtx, env.store, env.js) }()
	t.Cleanup(func() {
		cancelProj()
		select {
		case <-projDone:
		case <-time.After(5 * time.Second):
			t.Fatal("RunKVProjectors did not stop")
		}
	})

	keysKV := waitForBucket(t, env.js, controlplane.BucketKeys)

	waitForKV(t, func() (bool, error) {
		_, ok := getKeyEntry(t, keysKV, hash1)
		return ok, nil
	})
	entry, _ := getKeyEntry(t, keysKV, hash1)
	if entry.Org != "acme" || entry.Project != "proj-1" || entry.Name != "prod" ||
		len(entry.Allow) != 1 || entry.Allow[0] != "gpt-4" || entry.RateLimitRPM != 60 {
		t.Fatalf("hash1 entry after create = %+v, want Org=acme Project=proj-1 Name=prod Allow=[gpt-4] RateLimitRPM=60", entry)
	}

	// set allowlist
	if _, err := rt.Handle(context.Background(), stream, &controlplanev1.ApiKeyCommand{
		Kind: &controlplanev1.ApiKeyCommand_SetAllowlist{SetAllowlist: &controlplanev1.SetAllowlist{
			Allow: []string{"gpt-4", "claude-3"},
		}},
	}, es.Meta{}); err != nil {
		t.Fatalf("set allowlist: %v", err)
	}
	waitForKV(t, func() (bool, error) {
		e, ok := getKeyEntry(t, keysKV, hash1)
		return ok && len(e.Allow) == 2, nil
	})
	entry, _ = getKeyEntry(t, keysKV, hash1)
	if len(entry.Allow) != 2 || entry.Allow[0] != "gpt-4" || entry.Allow[1] != "claude-3" {
		t.Fatalf("hash1 entry after allowlist change = %+v, want Allow=[gpt-4 claude-3]", entry)
	}

	// rotate
	if _, err := rt.Handle(context.Background(), stream, &controlplanev1.ApiKeyCommand{
		Kind: &controlplanev1.ApiKeyCommand_Rotate{Rotate: &controlplanev1.RotateKey{NewHash: hash2}},
	}, es.Meta{}); err != nil {
		t.Fatalf("rotate: %v", err)
	}
	waitForKV(t, func() (bool, error) {
		_, ok := getKeyEntry(t, keysKV, hash2)
		return ok, nil
	})
	entry2, ok := getKeyEntry(t, keysKV, hash2)
	if !ok {
		t.Fatal("hash2 entry missing after rotate")
	}
	if len(entry2.Allow) != 2 || entry2.Allow[1] != "claude-3" || entry2.RateLimitRPM != 60 {
		t.Fatalf("hash2 entry after rotate = %+v, want carried-over Allow/RateLimitRPM", entry2)
	}
	if _, ok := getKeyEntry(t, keysKV, hash1); ok {
		t.Fatal("hash1 entry still present after rotate, want deleted")
	}

	// disable
	if _, err := rt.Handle(context.Background(), stream, &controlplanev1.ApiKeyCommand{
		Kind: &controlplanev1.ApiKeyCommand_Disable{Disable: &controlplanev1.DisableKey{}},
	}, es.Meta{}); err != nil {
		t.Fatalf("disable: %v", err)
	}
	waitForKV(t, func() (bool, error) {
		_, ok := getKeyEntry(t, keysKV, hash2)
		return !ok, nil
	})
}

// --- restart / replay catch-up -----------------------------------------

func TestKVProjectors_RestartReplay(t *testing.T) {
	relayCtx, cancelRelay := context.WithCancel(context.Background())
	defer cancelRelay()
	env := setupKVProjTest(t, relayCtx)

	rt := aggregate.NewRuntime(env.store, alias.Decider, alias.Codec())
	globalFast, err := es.NewStreamID(alias.StreamType, "g_fast")
	if err != nil {
		t.Fatalf("stream id: %v", err)
	}
	if _, err := rt.Handle(context.Background(), globalFast, &controlplanev1.AliasCommand{
		Kind: &controlplanev1.AliasCommand_Set{Set: &controlplanev1.SetAlias{Target: "llama"}},
	}, es.Meta{}); err != nil {
		t.Fatalf("set g_fast: %v", err)
	}

	// First run: catches the g_fast alias, then we stop it.
	proj1Ctx, cancelProj1 := context.WithCancel(context.Background())
	proj1Done := make(chan error, 1)
	go func() { proj1Done <- controlplane.RunKVProjectors(proj1Ctx, env.store, env.js) }()

	waitForKV(t, func() (bool, error) {
		kv, err := env.js.KeyValue(context.Background(), controlplane.BucketAliases)
		if err != nil {
			return false, err
		}
		_, ok := getAliasEntry(t, kv, "_global/fast")
		return ok, nil
	})

	cancelProj1()
	select {
	case <-proj1Done:
	case <-time.After(5 * time.Second):
		t.Fatal("first RunKVProjectors did not stop")
	}

	// While projectors are stopped, append one more alias event.
	acmeSmart, err := es.NewStreamID(alias.StreamType, "o_acme_smart")
	if err != nil {
		t.Fatalf("stream id: %v", err)
	}
	if _, err := rt.Handle(context.Background(), acmeSmart, &controlplanev1.AliasCommand{
		Kind: &controlplanev1.AliasCommand_Set{Set: &controlplanev1.SetAlias{Target: "claude"}},
	}, es.Meta{}); err != nil {
		t.Fatalf("set o_acme_smart: %v", err)
	}

	// Second run: must catch up without duplicating/erroring.
	proj2Ctx, cancelProj2 := context.WithCancel(context.Background())
	defer cancelProj2()
	proj2Done := make(chan error, 1)
	go func() { proj2Done <- controlplane.RunKVProjectors(proj2Ctx, env.store, env.js) }()
	t.Cleanup(func() {
		cancelProj2()
		select {
		case <-proj2Done:
		case <-time.After(5 * time.Second):
			t.Fatal("second RunKVProjectors did not stop")
		}
	})

	kv, err := env.js.KeyValue(context.Background(), controlplane.BucketAliases)
	if err != nil {
		t.Fatalf("bind ALIASES: %v", err)
	}
	waitForKV(t, func() (bool, error) {
		keys := bucketKeys(t, kv)
		return len(keys) == 2, nil
	})
	keys := bucketKeys(t, kv)
	if len(keys) != 2 || keys[0] != "_global/fast" || keys[1] != "acme/smart" {
		t.Fatalf("ALIASES bucket keys after restart+catchup = %v, want [_global/fast acme/smart]", keys)
	}
}

// --- resync -------------------------------------------------------------

func TestResyncKV(t *testing.T) {
	relayCtx, cancelRelay := context.WithCancel(context.Background())
	defer cancelRelay()
	env := setupKVProjTest(t, relayCtx)

	rt := aggregate.NewRuntime(env.store, alias.Decider, alias.Codec())
	globalFast, err := es.NewStreamID(alias.StreamType, "g_fast")
	if err != nil {
		t.Fatalf("stream id: %v", err)
	}
	if _, err := rt.Handle(context.Background(), globalFast, &controlplanev1.AliasCommand{
		Kind: &controlplanev1.AliasCommand_Set{Set: &controlplanev1.SetAlias{Target: "llama"}},
	}, es.Meta{}); err != nil {
		t.Fatalf("set g_fast: %v", err)
	}

	projCtx, cancelProj := context.WithCancel(context.Background())
	projDone := make(chan error, 1)
	go func() { projDone <- controlplane.RunKVProjectors(projCtx, env.store, env.js) }()

	waitForKV(t, func() (bool, error) {
		kv, err := env.js.KeyValue(context.Background(), controlplane.BucketAliases)
		if err != nil {
			return false, err
		}
		_, ok := getAliasEntry(t, kv, "_global/fast")
		return ok, nil
	})

	// Stop the projectors before resyncing so ResyncKV owns the buckets
	// exclusively (a live durable consumer racing a bucket delete+recreate
	// is not a scenario the projector needs to tolerate).
	cancelProj()
	select {
	case <-projDone:
	case <-time.After(5 * time.Second):
		t.Fatal("RunKVProjectors did not stop")
	}

	kv, err := env.js.KeyValue(context.Background(), controlplane.BucketAliases)
	if err != nil {
		t.Fatalf("bind ALIASES: %v", err)
	}
	if _, err := kv.PutString(context.Background(), "bogus/entry", "{}"); err != nil {
		t.Fatalf("put bogus entry: %v", err)
	}

	if err := controlplane.ResyncKV(context.Background(), env.store, env.js); err != nil {
		t.Fatalf("ResyncKV: %v", err)
	}

	kv, err = env.js.KeyValue(context.Background(), controlplane.BucketAliases)
	if err != nil {
		t.Fatalf("bind ALIASES after resync: %v", err)
	}
	keys := bucketKeys(t, kv)
	if len(keys) != 1 || keys[0] != "_global/fast" {
		t.Fatalf("ALIASES bucket keys after resync = %v, want exactly [_global/fast]", keys)
	}
	entry, ok := getAliasEntry(t, kv, "_global/fast")
	if !ok || entry.Target != "llama" {
		t.Fatalf("_global/fast entry after resync = %+v (ok=%v), want Target=llama", entry, ok)
	}
}
