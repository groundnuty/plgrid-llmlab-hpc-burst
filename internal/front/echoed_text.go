package front

// A message that repeats the client's own text repeats a bounded copy of it (issue #647). The front's error answers named what they refused: the model
// name, the provider part of it, the id in GET /v1/models/{id}, the deployment ID of a records request. They repeated it whole, so a request with a name
// of 5 MiB drew an answer of about 5 MiB, and a path of the header limit (1 MiB) drew one of that size, from a front that bounds what it records of the
// same text (recorded_text.go, issue #645).
//
// The copy is cut at the same place and in the same way as the usage record's: the longest name the front can serve, with the marker, at a character
// boundary, so nothing a request can be served under is cut, and a name at the bound is repeated whole. Only the message's text shortens, and only when
// the text is over the bound: the status, the code, the detail, the param and every header are the same, and so is every message that repeats nothing
// of the client's. HBEP-5's Errors table words the status, the code and the detail of each of these answers, and no text of a message; an error from a
// target, which passes through unchanged but for redaction, is not the front's message and is not touched.

// echoedName is the part of a client's name, id or path that an error message repeats: all of it up to maxRoutableNameBytes bytes, and the first
// maxRoutableNameBytes bytes and the marker for more.
func echoedName(s string) string { return boundedText(s, maxRoutableNameBytes) }
