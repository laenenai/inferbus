package gateway

import (
	"bytes"
	"encoding/json"
	"testing"
)

// TestMergeParams is the merge table from the M6 Task 3 brief: alias params
// override client-supplied values, each param value is typed by whether it
// parses as JSON (so "512" becomes the number 512, but "float" — which does
// not parse — stays the string "float"), and the reserved routing keys
// "model"/"stream" are never honored from an alias.
func TestMergeParams(t *testing.T) {
	tests := []struct {
		name   string
		body   string
		params map[string]string
		want   map[string]any // expected decoded body, compared field by field
	}{
		{
			name:   "number-valued param parses as JSON number",
			body:   `{"model":"m","input":"x"}`,
			params: map[string]string{"dimensions": "512"},
			want:   map[string]any{"model": "m", "input": "x", "dimensions": float64(512)},
		},
		{
			name:   "unparseable value stays a JSON string",
			body:   `{"model":"m","input":"x"}`,
			params: map[string]string{"encoding_format": "float"},
			want:   map[string]any{"model": "m", "input": "x", "encoding_format": "float"},
		},
		{
			name:   "boolean-valued param parses as JSON bool",
			body:   `{"model":"m","input":"x"}`,
			params: map[string]string{"normalize": "true"},
			want:   map[string]any{"model": "m", "input": "x", "normalize": true},
		},
		{
			name:   "array-valued param parses as JSON array",
			body:   `{"model":"m","input":"x"}`,
			params: map[string]string{"weights": "[1,2]"},
			want:   map[string]any{"model": "m", "input": "x", "weights": []any{float64(1), float64(2)}},
		},
		{
			name:   "object-valued param parses as JSON object",
			body:   `{"model":"m","input":"x"}`,
			params: map[string]string{"opts": `{"a":1}`},
			want:   map[string]any{"model": "m", "input": "x", "opts": map[string]any{"a": float64(1)}},
		},
		{
			name:   "alias param overrides a client-supplied value",
			body:   `{"model":"m","input":"x","dimensions":9}`,
			params: map[string]string{"dimensions": "512"},
			want:   map[string]any{"model": "m", "input": "x", "dimensions": float64(512)},
		},
		{
			name:   "reserved model/stream params are skipped",
			body:   `{"model":"m","input":"x"}`,
			params: map[string]string{"model": "evil", "stream": "true", "dimensions": "512"},
			want:   map[string]any{"model": "m", "input": "x", "dimensions": float64(512)},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			out, err := mergeParams([]byte(tt.body), tt.params)
			if err != nil {
				t.Fatalf("mergeParams: unexpected error: %v", err)
			}
			var got map[string]any
			if err := json.Unmarshal(out, &got); err != nil {
				t.Fatalf("merged body is not a JSON object: %v (body %s)", err, out)
			}
			if len(got) != len(tt.want) {
				t.Fatalf("merged body = %s, want exactly the keys of %v", out, tt.want)
			}
			for k, want := range tt.want {
				gotJSON, _ := json.Marshal(got[k])
				wantJSON, _ := json.Marshal(want)
				if !bytes.Equal(gotJSON, wantJSON) {
					t.Errorf("key %q = %s, want %s (full body %s)", k, gotJSON, wantJSON, out)
				}
			}
			// "stream" must stay absent when only the alias asked for it.
			if _, reserved := tt.params["stream"]; reserved {
				if _, present := got["stream"]; present {
					t.Errorf("reserved key stream leaked into merged body %s", out)
				}
			}
		})
	}
}

// TestMergeParamsNoParamsIsByteIdentical pins the nil/empty short-circuit:
// with nothing to merge the caller's body must come back untouched, not
// round-tripped through unmarshal/marshal (which would reorder keys and
// renormalize numbers on every single request that uses a plain alias).
func TestMergeParamsNoParamsIsByteIdentical(t *testing.T) {
	body := []byte(`{"model":"m",  "input":"x", "temperature":1.0000}`)
	for _, params := range []map[string]string{nil, {}} {
		out, err := mergeParams(body, params)
		if err != nil {
			t.Fatalf("mergeParams(%v): %v", params, err)
		}
		if !bytes.Equal(out, body) {
			t.Errorf("mergeParams(%v) = %s, want byte-identical %s", params, out, body)
		}
	}
}

// TestMergeParamsNonObjectBody: a body that is valid JSON but not an object
// cannot carry params — the caller turns this into a 400.
func TestMergeParamsNonObjectBody(t *testing.T) {
	if _, err := mergeParams([]byte(`[1]`), map[string]string{"dimensions": "512"}); err == nil {
		t.Fatal("mergeParams on a non-object body: want error, got nil")
	}
}

// TestMergeParamsReservedKeysAreCaseInsensitive guards a hang, not a style
// rule: encoding/json matches field names case-insensitively and lets the
// later duplicate win, so a body carrying "stream":false plus an
// alias-injected "Stream":true decodes to stream=true in the worker. The
// worker would then stream chunks while the gateway waited in resultOut for
// a single result frame, and the request would hang until its deadline. An
// exact-match reserved check let exactly that through.
func TestMergeParamsReservedKeysAreCaseInsensitive(t *testing.T) {
	body := []byte(`{"model":"real-target","stream":false,"input":"hi"}`)
	for _, key := range []string{"Stream", "STREAM", "sTrEaM", "Model", "MODEL"} {
		out, err := mergeParams(body, map[string]string{key: "true"})
		if err != nil {
			t.Fatalf("mergeParams with %q: %v", key, err)
		}
		var got map[string]any
		if err := json.Unmarshal(out, &got); err != nil {
			t.Fatalf("unmarshal result for %q: %v", key, err)
		}
		if _, present := got[key]; present {
			t.Errorf("reserved key %q was injected into the body: %s", key, out)
		}
		if got["stream"] != false {
			t.Errorf("param %q changed stream to %v (want false): %s", key, got["stream"], out)
		}
		if got["model"] != "real-target" {
			t.Errorf("param %q changed model to %v: %s", key, got["model"], out)
		}
	}
}
