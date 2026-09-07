package controlplane

import (
	"regexp"
	"testing"
)

const wantKeyPrefix = "ib_live_"

var hexRe = regexp.MustCompile(`^[0-9a-f]{64}$`)

func TestGenerateKey_HasExpectedPrefixAndLength(t *testing.T) {
	plaintext, _ := GenerateKey()

	if len(plaintext) < 40 {
		t.Fatalf("plaintext length = %d, want >= 40 (plaintext=%q)", len(plaintext), plaintext)
	}
	if plaintext[:len(wantKeyPrefix)] != wantKeyPrefix {
		t.Fatalf("plaintext = %q, want prefix %q", plaintext, wantKeyPrefix)
	}
}

func TestGenerateKey_TwoCallsDiffer(t *testing.T) {
	first, _ := GenerateKey()
	second, _ := GenerateKey()

	if first == second {
		t.Fatalf("two calls to GenerateKey produced the same plaintext %q", first)
	}
}

func TestGenerateKey_ReturnsHashAlongsidePlaintext(t *testing.T) {
	plaintext, hashHex := GenerateKey()

	want := HashKey(plaintext)
	if hashHex != want {
		t.Fatalf("GenerateKey hashHex = %q, want HashKey(plaintext) = %q", hashHex, want)
	}
}

func TestGenerateKey_HashIsLowercaseHex64(t *testing.T) {
	_, hashHex := GenerateKey()

	if !hexRe.MatchString(hashHex) {
		t.Fatalf("hashHex = %q, want 64 lowercase hex chars", hashHex)
	}
}

func TestHashKey_Deterministic(t *testing.T) {
	plaintext := "ib_live_deterministic-input-for-hash-test"

	if got, want := HashKey(plaintext), HashKey(plaintext); got != want {
		t.Fatalf("HashKey not deterministic: %q != %q", got, want)
	}
}
