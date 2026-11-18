package main

import (
	"encoding/json"
	"io"
	"time"
)

// Record is one request, written as one JSON line. It holds timings, counts and the attribution headers; nothing of a response body but
// numbers, and an error is a short class ("status 503"), never body text.
type Record struct {
	Workload string    `json:"workload"`
	Session  string    `json:"session"`
	Turn     int       `json:"turn"` // 1-based
	Start    time.Time `json:"start"`
	End      time.Time `json:"end"`
	Status   int       `json:"status"` // the HTTP status; 0 when there was no response
	// TTFTMs is the time from the request's start to the first content (or reasoning) delta; absent when none arrived.
	TTFTMs *float64 `json:"ttftMs,omitempty"`
	// PromptTokens and CompletionTokens are the endpoint's usage figures; absent when it sent none.
	PromptTokens     *int `json:"promptTokens,omitempty"`
	CompletionTokens *int `json:"completionTokens,omitempty"`
	// DecodeTokensPerSec is (completion_tokens - 1) over the time from the first to the last content delta; absent when it is undefined
	// (one token, or no usage).
	DecodeTokensPerSec *float64 `json:"decodeTokensPerSec,omitempty"`
	Attribution
	Error string `json:"error,omitempty"`
}

// recordWriter writes records as JSON lines. It is not safe for concurrent use; Run calls its sink from one goroutine.
type recordWriter struct{ enc *json.Encoder }

func newRecordWriter(w io.Writer) *recordWriter { return &recordWriter{enc: json.NewEncoder(w)} }

func (r *recordWriter) write(rec Record) error { return r.enc.Encode(rec) }

func ptrInt(v int) *int           { return &v }
func ptrFloat(v float64) *float64 { return &v }

func ms(d time.Duration) float64 { return float64(d) / float64(time.Millisecond) }
