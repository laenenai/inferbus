package gateway

import (
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
)

// reservedParams are the request-body keys an alias may never set. Both are
// routing decisions the gateway itself owns: "model" is what alias
// resolution just decided (letting an alias rewrite it would let one alias
// impersonate another target), and "stream" is what the handler already read
// off the client's request to pick between streamOut and resultOut — an
// alias flipping it after the fact would desynchronize the response shape
// from the transport the handler is committed to. The admin API rejects both
// at write time in a later task; this skip is defense in depth for entries
// that reached KV some other way (a hand-edited bucket, an older projector).
//
// Matching is CASE-INSENSITIVE, and that is load-bearing rather than
// tidiness: encoding/json matches struct fields case-insensitively, and a
// later duplicate key wins, so a body carrying both "stream":false and
// "Stream":true decodes to stream=true. An alias param spelled "Stream"
// would therefore slip past an exact-match check and flip the worker into
// ChatStream while the gateway is already committed to resultOut — the
// request would hang until its deadline. Keys here are lowercase; lookups
// lowercase the candidate.
var reservedParams = []string{"model", "stream"}

// isReservedParam reports whether k names a reserved body field, matching
// the way encoding/json itself matches field names: Unicode SIMPLE FOLDING,
// not ASCII lowercasing. The difference is exploitable, not academic —
// strings.ToLower leaves "ſtream" (U+017F LATIN SMALL LETTER LONG S)
// unchanged, so a ToLower guard lets it through, while encoding/json folds
// it onto "stream" and, because marshalling sorts keys and "ſtream" sorts
// after "stream", the injected value WINS the worker's stream probe.
func isReservedParam(k string) bool {
	for _, r := range reservedParams {
		if strings.EqualFold(k, r) {
			return true
		}
	}
	return false
}

// errBodyNotObject is what mergeParams returns for a body that is valid JSON
// but not a JSON object (an array, a bare number, ...). The chat handler
// turns it into a 400 invalid_request_error.
var errBodyNotObject = errors.New("request body must be a JSON object to carry alias params")

// mergeParams applies an alias's operator-pinned params onto a request body,
// overriding any client-supplied value for the same key — the alias is the
// operator's contract for what that name means, so it wins over whatever the
// caller sent.
//
// Params arrive as map[string]string because that is the shape the control
// plane stores them in (cpkv.AliasEntry.Params, an admin-API-supplied string
// map), but request bodies are typed JSON: "dimensions":"512" would be
// rejected by an upstream expecting a number. Each value is therefore typed
// by whether it parses as a JSON value on its own — "512" becomes 512, "true"
// becomes true, "[1,2]" and `{"a":1}` become the array/object they spell —
// and anything that does not parse (the common case of a plain enum word like
// "float") is inserted as a JSON string. This is unambiguous in the direction
// that matters: an operator who wants the literal string "512" cannot express
// it, but no such upstream parameter exists, whereas numeric/boolean
// parameters are everywhere.
//
// With no params to apply the body is returned exactly as given, byte for
// byte — the overwhelmingly common case (a plain alias) must not pay for a
// JSON round trip, which would also reorder keys and renormalize numbers in
// every request the gateway publishes.
func mergeParams(body []byte, params map[string]string) ([]byte, error) {
	if len(params) == 0 {
		return body, nil
	}

	var fields map[string]json.RawMessage
	if err := json.Unmarshal(body, &fields); err != nil {
		return nil, errBodyNotObject
	}
	if fields == nil {
		// Literal JSON null unmarshals into a nil map without erroring.
		return nil, errBodyNotObject
	}

	for k, v := range params {
		if isReservedParam(k) {
			slog.Warn("gateway: alias param ignored: reserved key", "key", k)
			continue
		}
		fields[k] = jsonValue(v)
	}

	out, err := json.Marshal(fields)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// jsonValue renders a param's string value as the JSON value to insert: the
// value itself when it is already a well-formed JSON value, otherwise a JSON
// string containing it. The Marshal of a plain string cannot fail, so the
// fallback needs no error path.
func jsonValue(v string) json.RawMessage {
	if json.Valid([]byte(v)) {
		return json.RawMessage(v)
	}
	quoted, err := json.Marshal(v)
	if err != nil { // unreachable: marshaling a string never fails
		return json.RawMessage(`""`)
	}
	return quoted
}
