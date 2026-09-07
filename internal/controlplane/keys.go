// Package controlplane implements the inferbus event-sourced control plane
// (org/apikey/alias aggregates) on es-lite v0.5.0.
package controlplane

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"math/big"
)

// keyPrefix marks a plaintext API key as live-issued by this control plane.
const keyPrefix = "ib_live_"

// base62Alphabet is used to render random key material as URL-safe,
// unambiguous-enough plaintext.
const base62Alphabet = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"

// GenerateKey creates a new API key: a plaintext credential to hand to the
// caller exactly once, and the hex-encoded SHA-256 hash of that plaintext to
// persist (the plaintext itself is never stored).
//
// plaintext is "ib_live_" followed by the base62 encoding of 32
// cryptographically random bytes (a fixed 43 base62 characters), so the
// returned string is always well over 40 characters and differs on every
// call. hashHex is HashKey(plaintext).
func GenerateKey() (plaintext, hashHex string) {
	var raw [32]byte
	if _, err := rand.Read(raw[:]); err != nil {
		// crypto/rand.Read only fails if the OS entropy source is
		// unavailable, which is unrecoverable for a security-sensitive
		// generator like this one.
		panic("controlplane: crypto/rand unavailable: " + err.Error())
	}

	plaintext = keyPrefix + base62Encode(raw[:])
	hashHex = HashKey(plaintext)
	return plaintext, hashHex
}

// HashKey returns the lowercase hex-encoded SHA-256 hash of plaintext. This
// is the form persisted in events and state (ApiKey.hash); the plaintext
// itself must never be stored.
func HashKey(plaintext string) string {
	sum := sha256.Sum256([]byte(plaintext))
	return hex.EncodeToString(sum[:])
}

// base62KeyLen is the rendered length of 32 random bytes in base62: 256 bits
// / log2(62) rounds up to 43 digits.
const base62KeyLen = 43

// base62Encode renders b as a fixed-width base62 string of base62KeyLen
// characters, using base62Alphabet. It treats b as a big-endian unsigned
// integer and left-pads with the alphabet's zero digit so the output length
// never varies with the number of leading zero bytes/bits in b.
func base62Encode(b []byte) string {
	n := new(big.Int).SetBytes(b)

	base := big.NewInt(int64(len(base62Alphabet)))
	mod := new(big.Int)
	out := make([]byte, 0, base62KeyLen)
	for n.Sign() > 0 {
		n.DivMod(n, base, mod)
		out = append(out, base62Alphabet[mod.Int64()])
	}
	for len(out) < base62KeyLen {
		out = append(out, base62Alphabet[0])
	}

	// DivMod produces digits least-significant-first; reverse for the
	// conventional most-significant-first rendering.
	for i, j := 0, len(out)-1; i < j; i, j = i+1, j-1 {
		out[i], out[j] = out[j], out[i]
	}
	return string(out)
}
