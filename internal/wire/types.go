package wire

import (
	"encoding/json"
	"time"
)

type Kind string

const (
	KindChunk  Kind = "chunk"
	KindDone   Kind = "done"
	KindResult Kind = "result"
	KindError  Kind = "error"
)

// Message is one frame on the response leg (core NATS, RespSubject).
type Message struct {
	Kind    Kind            `json:"kind"`
	Seq     int             `json:"seq"`
	Payload json.RawMessage `json:"payload,omitempty"`
	Error   *WireError      `json:"error,omitempty"`
	Usage   *Usage          `json:"usage,omitempty"`
}

type WireError struct {
	Code       string `json:"code"`
	Message    string `json:"message"`
	HTTPStatus int    `json:"http_status"`
}

type Usage struct {
	PromptTokens     int `json:"prompt_tokens"`
	CompletionTokens int `json:"completion_tokens"`
	CachedTokens     int `json:"cached_tokens"`
}

// UsageEvent is one accounting record on the METERING stream.
type UsageEvent struct {
	ReqID     string `json:"req_id"`
	Org       string `json:"org"`
	Project   string `json:"project"`
	KeyID     string `json:"key_id"`
	Alias     string `json:"alias"`
	Model     string `json:"model"`
	Provider  string `json:"provider"`
	Kind      string `json:"kind"`   // chat|embed
	Status    string `json:"status"` // ok|error|canceled
	ErrorCode string `json:"error_code,omitempty"`
	WorkerID  string `json:"worker_id"`
	Usage
	TTFTMillis     int64     `json:"ttft_ms"`
	DurationMillis int64     `json:"duration_ms"`
	QueueMillis    int64     `json:"queue_ms"`
	Estimated      bool      `json:"estimated"`
	TS             time.Time `json:"ts"`
}

// WorkerAd is a worker's advertisement in the MODELS KV bucket
// (key = worker id). Best-effort presence data: entries expire via the
// bucket TTL when a worker stops heartbeating.
type WorkerAd struct {
	WorkerID  string          `json:"worker_id"`
	Models    []WorkerAdModel `json:"models"`
	StartedAt time.Time       `json:"started_at"`
	LastSeen  time.Time       `json:"last_seen"`
}

type WorkerAdModel struct {
	Name        string `json:"name"`
	Engine      string `json:"engine"`
	MaxInflight int    `json:"max_inflight"`
}
