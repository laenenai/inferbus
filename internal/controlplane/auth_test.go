package controlplane

import (
	"context"
	"net/http"
	"os"
	"testing"
)

// fakeVerifier is a hermetic stand-in for the real OIDC verifier: it maps a
// raw "JWT" string directly to a subject, with no network or crypto
// involved, so unit tests never depend on an issuer.
type fakeVerifier struct {
	subs map[string]string // rawJWT -> sub
}

func (f *fakeVerifier) Verify(_ context.Context, rawJWT string) (string, error) {
	sub, ok := f.subs[rawJWT]
	if !ok {
		return "", errFakeVerifyFailed
	}
	return sub, nil
}

var errFakeVerifyFailed = errFake("fake verifier: token not recognized")

type errFake string

func (e errFake) Error() string { return string(e) }

func newReq(t *testing.T, bearer string) *http.Request {
	t.Helper()
	r, err := http.NewRequest(http.MethodGet, "/", nil)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	if bearer != "" {
		r.Header.Set("Authorization", "Bearer "+bearer)
	}
	return r
}

func TestAuthenticate_BootstrapTokenAccepted(t *testing.T) {
	cfg := Config{BootstrapToken: "s3cret"}
	auth := NewAuthenticator(cfg, &fakeVerifier{})

	id, err := auth.Authenticate(newReq(t, "s3cret"))
	if err != nil {
		t.Fatalf("Authenticate: unexpected error: %v", err)
	}
	if id.Sub != "bootstrap" {
		t.Errorf("Sub = %q, want %q", id.Sub, "bootstrap")
	}
	if !id.PlatformAdmin {
		t.Errorf("PlatformAdmin = false, want true for bootstrap identity")
	}
}

func TestAuthenticate_BootstrapTokenWrongIs401(t *testing.T) {
	cfg := Config{BootstrapToken: "s3cret"}
	auth := NewAuthenticator(cfg, &fakeVerifier{})

	_, err := auth.Authenticate(newReq(t, "not-the-secret"))
	if err == nil {
		t.Fatal("Authenticate: expected error for wrong bootstrap token, got nil")
	}
}

func TestAuthenticate_BootstrapDisabledWhenConfigEmpty(t *testing.T) {
	cfg := Config{BootstrapToken: ""}
	auth := NewAuthenticator(cfg, &fakeVerifier{})

	// Even presenting an empty-string token must not authenticate when the
	// bootstrap token is disabled: an empty presented token compared against
	// an empty configured token must never succeed.
	_, err := auth.Authenticate(newReq(t, ""))
	if err == nil {
		t.Fatal("Authenticate: expected error when bootstrap token is disabled (empty config)")
	}

	_, err = auth.Authenticate(newReq(t, "anything"))
	if err == nil {
		t.Fatal("Authenticate: expected error when bootstrap token is disabled (empty config), got success for arbitrary token")
	}
}

func TestAuthenticate_MissingAuthorizationHeader(t *testing.T) {
	cfg := Config{BootstrapToken: "s3cret"}
	auth := NewAuthenticator(cfg, &fakeVerifier{})

	_, err := auth.Authenticate(newReq(t, ""))
	if err == nil {
		t.Fatal("Authenticate: expected error for missing Authorization header")
	}
}

func TestAuthenticate_FakeVerifierIdentity_NonAdmin(t *testing.T) {
	cfg := Config{PlatformAdmins: []string{"admin-sub"}}
	auth := NewAuthenticator(cfg, &fakeVerifier{subs: map[string]string{
		"jwt-for-alice": "alice",
	}})

	id, err := auth.Authenticate(newReq(t, "jwt-for-alice"))
	if err != nil {
		t.Fatalf("Authenticate: unexpected error: %v", err)
	}
	if id.Sub != "alice" {
		t.Errorf("Sub = %q, want %q", id.Sub, "alice")
	}
	if id.PlatformAdmin {
		t.Errorf("PlatformAdmin = true, want false: alice is not in PlatformAdmins")
	}
}

func TestAuthenticate_FakeVerifierIdentity_PlatformAdminMatch(t *testing.T) {
	cfg := Config{PlatformAdmins: []string{"admin-sub", "another-admin"}}
	auth := NewAuthenticator(cfg, &fakeVerifier{subs: map[string]string{
		"jwt-for-admin": "admin-sub",
	}})

	id, err := auth.Authenticate(newReq(t, "jwt-for-admin"))
	if err != nil {
		t.Fatalf("Authenticate: unexpected error: %v", err)
	}
	if id.Sub != "admin-sub" {
		t.Errorf("Sub = %q, want %q", id.Sub, "admin-sub")
	}
	if !id.PlatformAdmin {
		t.Errorf("PlatformAdmin = false, want true: admin-sub is in PlatformAdmins")
	}
}

func TestAuthenticate_FakeVerifierRejectsUnknownToken(t *testing.T) {
	cfg := Config{}
	auth := NewAuthenticator(cfg, &fakeVerifier{subs: map[string]string{
		"jwt-for-alice": "alice",
	}})

	_, err := auth.Authenticate(newReq(t, "not-a-known-jwt"))
	if err == nil {
		t.Fatal("Authenticate: expected error for a token the verifier does not recognize")
	}
}

func TestAuthenticate_BootstrapTakesPriorityOverVerifier(t *testing.T) {
	// When both a bootstrap token is configured and the presented token
	// happens to also be a value the verifier recognizes, the bootstrap
	// comparison is the one that must match: this test simply confirms the
	// bootstrap token still authenticates when a verifier is also wired up.
	cfg := Config{BootstrapToken: "s3cret", PlatformAdmins: []string{"alice"}}
	auth := NewAuthenticator(cfg, &fakeVerifier{subs: map[string]string{
		"jwt-for-alice": "alice",
	}})

	id, err := auth.Authenticate(newReq(t, "s3cret"))
	if err != nil {
		t.Fatalf("Authenticate: unexpected error: %v", err)
	}
	if id.Sub != "bootstrap" || !id.PlatformAdmin {
		t.Errorf("Identity = %+v, want bootstrap platform-admin identity", id)
	}
}

func TestAuthorize_TruthTable(t *testing.T) {
	admin := Identity{Sub: "u", PlatformAdmin: true}
	nonAdmin := Identity{Sub: "u", PlatformAdmin: false}

	tests := []struct {
		name    string
		id      Identity
		orgRole string
		need    string
		want    bool
	}{
		// Platform admin: always true, regardless of org role or need.
		{"platform admin, no org role, read", admin, "", "read", true},
		{"platform admin, viewer role, manage_org", admin, "viewer", "manage_org", true},
		{"platform admin, empty role, manage_keys", admin, "", "manage_keys", true},

		// owner: all needs.
		{"owner read", nonAdmin, "owner", "read", true},
		{"owner manage_keys", nonAdmin, "owner", "manage_keys", true},
		{"owner manage_org", nonAdmin, "owner", "manage_org", true},

		// admin: read + manage_keys, not manage_org.
		{"admin read", nonAdmin, "admin", "read", true},
		{"admin manage_keys", nonAdmin, "admin", "manage_keys", true},
		{"admin manage_org", nonAdmin, "admin", "manage_org", false},

		// viewer: read only.
		{"viewer read", nonAdmin, "viewer", "read", true},
		{"viewer manage_keys", nonAdmin, "viewer", "manage_keys", false},
		{"viewer manage_org", nonAdmin, "viewer", "manage_org", false},

		// unknown/empty role for a non-admin: no access.
		{"unknown role read", nonAdmin, "member", "read", false},
		{"empty role read", nonAdmin, "", "read", false},
		{"empty role manage_keys", nonAdmin, "", "manage_keys", false},
		{"empty role manage_org", nonAdmin, "", "manage_org", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := Authorize(tt.id, tt.orgRole, tt.need)
			if got != tt.want {
				t.Errorf("Authorize(%+v, %q, %q) = %v, want %v", tt.id, tt.orgRole, tt.need, got, tt.want)
			}
		})
	}
}

// TestNewOIDCVerifier_Discovery is env-gated: it exercises real issuer
// discovery against a live OIDC issuer and is skipped unless
// CP_TEST_OIDC_ISSUER is set, keeping the rest of this package's tests
// hermetic and network-free.
func TestNewOIDCVerifier_Discovery(t *testing.T) {
	issuer := os.Getenv("CP_TEST_OIDC_ISSUER")
	if issuer == "" {
		t.Skip("CP_TEST_OIDC_ISSUER not set; skipping live OIDC discovery test")
	}
	ctx := context.Background()
	v, err := NewOIDCVerifier(ctx, issuer, "test-audience")
	if err != nil {
		t.Fatalf("NewOIDCVerifier: %v", err)
	}
	if v == nil {
		t.Fatal("NewOIDCVerifier returned nil verifier with nil error")
	}
}
