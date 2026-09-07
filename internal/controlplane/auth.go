// Package controlplane: authn/authz for the control-plane API.
//
// Two ways to authenticate a request:
//
//   - Bootstrap token: a single shared secret (Config.BootstrapToken)
//     presented as "Authorization: Bearer <token>", compared in constant
//     time. It is disabled entirely when the config value is empty — an
//     empty presented token must never match an empty configured one.
//     A successful bootstrap match always yields a platform-admin identity.
//   - OIDC: the bearer value is a JWT verified by a TokenVerifier, which
//     returns the token's subject. Subjects listed in Config.PlatformAdmins
//     are treated as platform admins; all others are ordinary identities
//     whose org-level access is decided by Authorize.
//
// TokenVerifier is a seam: production wires NewOIDCVerifier (issuer
// discovery + audience-checked signature verification via go-oidc), while
// unit tests use a hermetic fake so this package's tests never touch the
// network.
package controlplane

import (
	"context"
	"crypto/subtle"
	"errors"
	"net/http"
	"strings"

	"github.com/coreos/go-oidc/v3/oidc"
)

// Identity is the authenticated caller of a control-plane request.
type Identity struct {
	// Sub is the caller's subject: "bootstrap" for the bootstrap token, or
	// the OIDC token's "sub" claim otherwise.
	Sub string
	// PlatformAdmin is true for the bootstrap identity and for any OIDC
	// subject listed in Config.PlatformAdmins. Platform admins bypass
	// per-org role checks in Authorize.
	PlatformAdmin bool
}

// ErrUnauthenticated is returned by Authenticate when the request carries no
// valid credential (missing header, wrong bootstrap token, or a JWT the
// verifier rejects).
var ErrUnauthenticated = errors.New("controlplane: unauthenticated")

// TokenVerifier verifies a raw JWT and returns its subject. It is the seam
// that keeps OIDC (network calls, key discovery, signature checks) out of
// this package's unit tests: production code wires NewOIDCVerifier, tests
// wire a fake.
type TokenVerifier interface {
	Verify(ctx context.Context, rawJWT string) (sub string, err error)
}

// Authenticator authenticates incoming control-plane requests.
type Authenticator interface {
	Authenticate(r *http.Request) (Identity, error)
}

// Auth is the default Authenticator: bootstrap token first, OIDC via
// TokenVerifier otherwise.
type Auth struct {
	cfg      Config
	verifier TokenVerifier
}

// NewAuthenticator builds an Auth from config and a TokenVerifier. Pass
// NewOIDCVerifier's result in production, or a fake in tests.
func NewAuthenticator(cfg Config, verifier TokenVerifier) *Auth {
	return &Auth{cfg: cfg, verifier: verifier}
}

// Authenticate extracts the bearer token from the request's Authorization
// header and resolves it to an Identity, trying the bootstrap token first
// (when configured) and falling back to OIDC verification.
//
// Never logs the presented token or JWT.
func (a *Auth) Authenticate(r *http.Request) (Identity, error) {
	rawToken, ok := bearerToken(r)
	if !ok {
		return Identity{}, ErrUnauthenticated
	}

	if a.cfg.BootstrapToken != "" {
		presented := []byte(rawToken)
		configured := []byte(a.cfg.BootstrapToken)
		// ConstantTimeCompare short-circuits (returns 0 immediately, no
		// byte-by-byte comparison) when the two lengths differ, so it does
		// leak the configured token's length via timing. Accepted: the
		// bootstrap token is a high-entropy secret, and length alone is not
		// exploitable.
		if subtle.ConstantTimeCompare(presented, configured) == 1 {
			return Identity{Sub: "bootstrap", PlatformAdmin: true}, nil
		}
	}

	sub, err := a.verifier.Verify(r.Context(), rawToken)
	if err != nil {
		return Identity{}, ErrUnauthenticated
	}

	return Identity{Sub: sub, PlatformAdmin: isPlatformAdmin(a.cfg.PlatformAdmins, sub)}, nil
}

func isPlatformAdmin(admins []string, sub string) bool {
	for _, a := range admins {
		if a == sub {
			return true
		}
	}
	return false
}

// bearerToken extracts the token from an "Authorization: Bearer <token>"
// header, following the gateway's convention (see
// internal/gateway.Gateway.authenticate).
func bearerToken(r *http.Request) (string, bool) {
	auth := r.Header.Get("Authorization")
	const prefix = "Bearer "
	if !strings.HasPrefix(auth, prefix) || len(auth) <= len(prefix) {
		return "", false
	}
	return auth[len(prefix):], true
}

// Authorize decides whether id may perform need against an org where the
// caller holds orgRole (looked up by the admin layer via
// ReadStore.GetOrg). need is one of "read", "manage_keys", or
// "manage_org".
//
// Platform admins are always authorized. Otherwise access follows the
// caller's org role:
//
//   - owner: read, manage_keys, manage_org
//   - admin: read, manage_keys
//   - viewer: read
//
// Any other role (including no membership, i.e. an empty orgRole) is
// denied.
func Authorize(id Identity, orgRole string, need string) bool {
	if id.PlatformAdmin {
		return true
	}
	switch orgRole {
	case "owner":
		switch need {
		case "read", "manage_keys", "manage_org":
			return true
		}
	case "admin":
		switch need {
		case "read", "manage_keys":
			return true
		}
	case "viewer":
		switch need {
		case "read":
			return true
		}
	}
	return false
}

// oidcVerifier is the production TokenVerifier: issuer discovery plus
// audience- and signature-checked JWT verification via go-oidc.
type oidcVerifier struct {
	verifier *oidc.IDTokenVerifier
}

// NewOIDCVerifier performs OIDC issuer discovery against issuer and returns
// a TokenVerifier that checks tokens are signed by that issuer for the
// given audience.
func NewOIDCVerifier(ctx context.Context, issuer, audience string) (TokenVerifier, error) {
	provider, err := oidc.NewProvider(ctx, issuer)
	if err != nil {
		return nil, err
	}
	verifier := provider.Verifier(&oidc.Config{ClientID: audience})
	return &oidcVerifier{verifier: verifier}, nil
}

// Verify checks rawJWT's signature, issuer, expiry, and audience, and
// returns its subject. It never logs the token.
func (v *oidcVerifier) Verify(ctx context.Context, rawJWT string) (string, error) {
	idToken, err := v.verifier.Verify(ctx, rawJWT)
	if err != nil {
		return "", err
	}
	return idToken.Subject, nil
}
