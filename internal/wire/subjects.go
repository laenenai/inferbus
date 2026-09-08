// Package wire is the single NATS contract shared by gateway, worker,
// and harvester: subjects, streams, headers, and message types.
package wire

import (
	"strings"
	"time"
)

const (
	StreamInference = "INFERENCE"
	StreamMetering  = "METERING"
	BucketModels    = "MODELS"

	HdrOrg      = "Ib-Org"
	HdrProject  = "Ib-Project"
	HdrKeyID    = "Ib-Key-Id"
	HdrAlias    = "Ib-Alias"
	HdrReqID    = "Ib-Req-Id"
	HdrKind     = "Ib-Kind"
	HdrReply    = "Ib-Reply"
	HdrDeadline = "Ib-Deadline" // RFC3339Nano
	HdrWorkerID = "Ib-Worker-Id"

	ModelsTTL       = 45 * time.Second
	ModelsHeartbeat = 15 * time.Second
)

// Slug maps a model name to a NATS-subject-safe token: lowercase,
// [a-z0-9-], every other rune collapsed to '-'.
func Slug(model string) string {
	var b strings.Builder
	prevDash := false
	for _, r := range strings.ToLower(model) {
		ok := r >= 'a' && r <= 'z' || r >= '0' && r <= '9' || r == '-'
		if !ok {
			r = '-'
		}
		if r == '-' && prevDash {
			continue
		}
		prevDash = r == '-'
		b.WriteRune(r)
	}
	return strings.Trim(b.String(), "-")
}

func ReqSubject(model string) string    { return "inference.req." + Slug(model) }
func RespSubject(reqID string) string   { return "inference.resp." + reqID }
func CancelSubject(reqID string) string { return "inference.cancel." + reqID }
func Durable(model string) string       { return "model-" + Slug(model) }

func UsageSubject(org, project, model string) string {
	return "metering.usage." + Slug(org) + "." + Slug(project) + "." + Slug(model)
}
