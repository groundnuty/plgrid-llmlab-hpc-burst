// Package llmlabtest is an HTTP double of PLGrid LLMLab, the managed gateway:
// an OpenAI-compatible server on loopback whose field behaviours a test
// switches on one at a time (HBEP-4, Test plan, "Managed fake behaviours";
// HBEP-5, Test plan). The managed Provider's controller (#69) and the front
// (#64) test against it; both import this package, and nothing copies it.
// It uses the standard library only.
//
// A fresh Server is a well-behaved gateway: it answers at once, never stalls,
// never cuts, and passes requests unchanged. Each behaviour below is off until
// a test turns it on, and can be turned off again:
//
//   - The no-byte cut (L-6.1): SetNoByteCut. A reply that has sent no byte
//     when the cut passes ends with 504; a stream that goes quiet for the cut
//     after its first byte ends without [DONE]. A stream's status line is
//     flushed at once, on every route, so a stream that is quiet from the
//     start is already a mid-stream cut. LLMLab's cut is 180 s (NoByteCut;
//     L-6.1, HBEP-5, The LLMLab route).
//   - Stalls (L-6.2): SetStalls for periodic windows, Stall for a manual one.
//     A request that arrives in a stall sends its first byte only when the
//     stall ends, so a long stall meets the no-byte cut. L-6.2 names a sync
//     every 30 min (StallPeriod); it gives no length, so a test sets one.
//     Stalls hold every route, the listing included ("all-model stalls").
//   - Models going inactive (L-6.5): SetActive. An inactive model stays in
//     GET /v1/models, because the listing does not show the state, and its
//     requests get 503 (HBEP-5, Notes: "503 for an inactive model").
//   - Ambiguous errors (L-6.5): an unknown model and a grant-blocked one
//     (SetBlocked) always get one shared 400 body; with SetAmbiguous an
//     inactive model gets that same 400 instead of 503, so a client cannot
//     tell the three causes apart.
//   - Key expiry and rejection (L-6.4): ExpireKeyAt makes a key fail with 401
//     from a time on; RejectKey answers 401 or 403 for it. An unknown or
//     missing key is 401. EchoKey makes every error body carry the
//     Authorization header the request sent, so a test can show that a
//     client never copies a body into a log, an event or a status.
//   - Request rewriting (L-6.6): SetRewriting. The gateway adds a fixed
//     prompt segment (InjectedTokens, 10 to 15 tokens in L-6.6), which shows
//     in usage.prompt_tokens; refuses a body member outside its allowlist
//     with 422 (L-6.6 names cache_salt); and refuses max_tokens equal to the
//     model's context window with 400, before counting tokens.
//   - Listing changes: SetModels, AddModel and RemoveModel. A removed model
//     is unknown, so it gets the ambiguous 400; AddModel brings it back with
//     the state it had.
//
// Two further behaviours ride on the model: Model.MaxTokensCap, a hidden
// server-side cap that ends output early with finish reason "length" (L-6.5),
// and Model.TruncateAfter, a stream that ends after that many content chunks
// with no finish reason and no [DONE] (L-6.7).
//
// # What is known and what is assumed
//
// The status codes and timings above come from the lessons (docs/lessons,
// L-6.1 to L-6.8) and HBEP-5. The bodies are not: no capture of LLMLab's
// error bodies is public, so every error body here is an assumed
// OpenAI-style shape, {"error": {"message", "type", "param", "code"}}, and a
// test must not treat its wording as LLMLab's. The listing is OpenAI's
// {"object": "list", "data": [{"id", "object": "model", "created",
// "owned_by"}]}. OpenAI's model object has no revision, and L-6.8 says
// LLMLab's revisions cannot be read; a Model with a Revision adds a
// "revision" member, which is this package's assumption, shared with the
// controller's reader (#69).
//
// # Routes
//
//   - GET /v1/models and GET /v1/models/{id}: the listing and one entry.
//   - POST /v1/chat/completions, /v1/completions, /v1/responses (stream or
//     not) and /v1/embeddings. Output is the reply text (SetReply), one word
//     per token and per stream chunk; prompt tokens are the
//     whitespace-separated words of the request's text, plus the injected
//     segment when rewriting is on. A stream sends usage when the request
//     asks for it (stream_options.include_usage), as OpenAI's does. A
//     Responses stream ends with response.completed, or with
//     response.incomplete when the output was capped (max_output_tokens or
//     Model.MaxTokensCap), as OpenAI's does.
//   - Anything else is 404, and a wrong method 405.
//
// Every request is logged with its raw body and headers as they arrived
// (Requests), so the front's tests can compare bytes ("No change but model",
// HBEP-5, Test plan).
//
// # What a test may assert about absence
//
// A request is logged when its answer ends. Requests and Count therefore do
// not see a request that a stall, a latency or the cut still holds, and an
// assertion that "the gateway got nothing" made with them passes whenever the
// request is held. Use Arrived for such an assertion: it counts a request at
// its arrival, answered or not. To know a held request has really met the
// wait before a test releases it, poll Waiting, the number of requests now
// blocked in a stall, the first-byte latency or a stream's pacing, instead of
// sleeping and hoping.
//
// # Time
//
// Expiry, stall windows, the first-byte delay and the cut all read
// Options.Now (time.Now by default). A waiting request re-reads the clock
// every few milliseconds and on every change a test makes, so a test with a
// manual clock releases it by moving the clock. Every wait also ends when the
// client goes away or the Server closes, so Close never hangs. Options.Now is
// called from the handler goroutines, some of them with the Server's lock
// held, so it must be safe for concurrent use and must never call a method of
// the Server.
//
// Latency.PerChunk paces the chunks of a stream only; a reply that is not
// streamed ignores it. Model a slow non-streamed reply with Latency.FirstByte.
//
// # Ports
//
// The Server binds 127.0.0.1 only, on the first free port of the range the
// test gives (Options.PortLo to PortHi): each issue has its own block on the
// shared machine. Listen does the same for any other listener a test needs.
package llmlabtest
