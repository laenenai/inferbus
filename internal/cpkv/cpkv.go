// Package cpkv holds the KV-projection schema shared between the control
// plane (the writer, internal/controlplane/kvproj.go) and the gateway (a
// pure reader, internal/gateway/kviam.go): the two bucket names, their JSON
// value shapes, the global alias scope, and the key-hashing scheme.
//
// This lives in its own package (review ruling I5) so the gateway can
// depend on exactly this — and nothing else — instead of the whole
// internal/controlplane package, which pulls in Postgres (pgx) and OIDC
// (go-oidc) for its admin API and read models. None of that has anything
// to do with reading two KV buckets, and a gateway binary has no business
// linking it in. internal/controlplane re-exports every symbol here as a
// type/const/func alias, so every existing controlplane.X reference (in
// controlplane's own code and tests) keeps compiling unchanged.
package cpkv

import (
	"crypto/sha256"
	"encoding/hex"
)

// Bucket names for the two KV projections.
const (
	BucketAliases = "ALIASES"
	BucketKeys    = "KEYS"
)

// GlobalScope is the ALIASES bucket's cross-org default scope: an entry
// under "_global/<name>" applies to any org with no "<org>/<name>"
// override of its own.
const GlobalScope = "_global"

// AliasEntry is the ALIASES bucket's value schema (JSON), stored under key
// "<scope>/<name>".
type AliasEntry struct {
	Target string            `json:"target"`
	Params map[string]string `json:"params,omitempty"`
}

// KeyEntry is the KEYS bucket's value schema (JSON), stored under key =
// the API key's current hash hex.
type KeyEntry struct {
	Org          string   `json:"org"`
	Project      string   `json:"project"`
	Name         string   `json:"name"`
	Allow        []string `json:"allow,omitempty"`
	RateLimitRPM int      `json:"rate_limit_rpm,omitempty"`

	// Disabled is reserved: today KeyDisabled deletes the entry entirely
	// (spec §4 "KeyDisabled -> Delete current_hash") rather than flagging
	// it, so this field is always false in the current projector. It is
	// kept in the schema for a possible future soft-disable read model.
	Disabled bool `json:"disabled,omitempty"`
}

// HashKey returns the lowercase hex-encoded SHA-256 hash of plaintext. This
// is the form persisted in events and state (ApiKey.hash) and the form the
// KEYS bucket is keyed by; the plaintext itself must never be stored.
func HashKey(plaintext string) string {
	sum := sha256.Sum256([]byte(plaintext))
	return hex.EncodeToString(sum[:])
}
