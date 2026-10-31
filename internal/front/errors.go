package front

import (
	"bytes"
	"encoding/json"
	"net/http"
)

// The codes of HBEP-5's Errors table.
const (
	codeInvalidAPIKey       = "invalid_api_key"
	codeInvalidRequest      = "invalid_request"
	codeInvalidModelName    = "invalid_model_name"
	codeModelInactive       = "model_inactive"
	codeCapacity            = "capacity"
	codeUpstreamUnreachable = "upstream_unreachable"
	codeRequestTimeout      = "request_timeout"
	codeStreamCut           = "stream_cut"
	// codeUpstreamFailed is the front's: the connection broke after the
	// request was written and before any response header. HBEP-5's table has
	// no row for it; the request is not sent again (open question 5).
	codeUpstreamFailed = "upstream_failed"
)

// statusClientClosed records a request whose client went away before an
// answer; nothing is sent, the number is nginx's 499 and is only recorded.
const statusClientClosed = 499

// errorBody is the body of every error the front raises (HBEP-5, Errors).
type errorBody struct {
	Error errorFields `json:"error"`
}

type errorFields struct {
	Message string  `json:"message"`
	Type    string  `json:"type"`
	Code    string  `json:"code"`
	Param   *string `json:"param"`
	Detail  *string `json:"detail"`
}

func encodeError(code, detail, message string, param string) []byte {
	f := errorFields{Message: message, Type: "hpcburst_error", Code: code}
	if detail != "" {
		f.Detail = &detail
	}
	if param != "" {
		f.Param = &param
	}
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	_ = enc.Encode(errorBody{Error: f})
	return bytes.TrimRight(buf.Bytes(), "\n")
}

// streamErrorEvent is the one event that ends a stream the front judged cut or
// truncated: a data line with the error body, and never [DONE].
func streamErrorEvent(detail, message string) []byte {
	b := encodeError(codeStreamCut, detail, message, "")
	out := make([]byte, 0, len(b)+8)
	out = append(out, "data: "...)
	out = append(out, b...)
	return append(out, '\n', '\n')
}

// Attribution headers (HBEP-5, Attribution headers; the owner's names of 30
// Oct 2026).
const (
	HeaderRequestID    = "Hpcburst-Request-Id"
	HeaderEmitter      = "Hpcburst-Emitter"
	HeaderProvider     = "Hpcburst-Provider"
	HeaderDeploymentID = "Hpcburst-Deployment-Id"
	HeaderInstance     = "Hpcburst-Instance"
	HeaderInstanceUID  = "Hpcburst-Instance-Uid"
)

// Emitters.
const (
	emitterFront    = "front"
	emitterInstance = "instance"
	emitterProvider = "provider"
)

// attribution is what a response names. An error the front raises before it
// chooses a target names only the request ID and emitter front.
type attribution struct {
	requestID    string
	emitter      string
	provider     string
	deploymentID string // display form; a managed entry's managed ID
	instance     string // namespace/name
	instanceUID  string
}

func (a attribution) set(h http.Header) {
	h.Set(HeaderRequestID, a.requestID)
	h.Set(HeaderEmitter, a.emitter)
	for k, v := range map[string]string{
		HeaderProvider: a.provider, HeaderDeploymentID: a.deploymentID,
		HeaderInstance: a.instance, HeaderInstanceUID: a.instanceUID,
	} {
		if v != "" {
			h.Set(k, v)
		} else {
			h.Del(k)
		}
	}
}

// forTarget is the attribution of a chosen target; emitter says who produced
// the response (the target, or the front for its own error about it).
func forTarget(id string, t *target, emitter string) attribution {
	a := attribution{requestID: id, emitter: emitter}
	switch {
	case t == nil:
	case t.managed != nil:
		a.provider, a.deploymentID = t.managed.Provider, t.managed.DisplayID
	case t.inst != nil:
		a.provider, a.deploymentID = t.inst.Provider, t.inst.DisplayID
		a.instance, a.instanceUID = t.inst.Key, string(t.inst.UID)
	}
	return a
}
