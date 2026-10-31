package hubfiles

import (
	"fmt"
	"strings"
)

// linkParam is one parameter of a Link value. hasValue is false for a
// parameter written without "=".
type linkParam struct {
	name     string // lower-cased: parameter names are case-insensitive
	value    string // the token, or the quoted string with its escapes resolved
	hasValue bool
}

type linkValue struct {
	target string
	params []linkParam
}

// nextLinkTarget returns the target of the rel="next" link among the values
// of a response's Link header fields (RFC 8288), or "" when there is none.
//
// The header is parsed with its quoting rules: a comma or a semicolon inside
// a quoted parameter (title="page 2, continued") or inside the <target> does
// not end the value or the parameter, so it can neither hide a next link nor
// make one up. A header that does not parse, a next link whose relation or
// anchor is ambiguous, and two different next links are errors, never "no
// next page": the end of a listing must be a decision of the Hub's, not a
// parser's.
func nextLinkTarget(values []string) (string, error) {
	var found []string
	for _, v := range values {
		links, err := parseLinkHeader(v)
		if err != nil {
			return "", fmt.Errorf("%w: Link header: %v", ErrInvalidListing, err)
		}
		for _, l := range links {
			next, err := hasNextRelation(l)
			if err != nil {
				return "", fmt.Errorf("%w: Link header: %v", ErrInvalidListing, err)
			}
			if !next {
				continue
			}
			if l.target == "" {
				return "", fmt.Errorf("%w: Link header: a next link with an empty target", ErrInvalidListing)
			}
			found = append(found, l.target)
		}
	}
	if len(found) == 0 {
		return "", nil
	}
	for _, f := range found[1:] {
		if f != found[0] {
			return "", fmt.Errorf("%w: Link header: two different next links, %q and %q", ErrInvalidListing, found[0], f)
		}
	}
	return found[0], nil
}

// hasNextRelation reports whether l carries the relation type "next". Only
// the first rel parameter counts under RFC 8288, but a second one with a
// different meaning would be a silent change of reading, so it is an error.
// A next link with an anchor parameter is a link of some other resource.
func hasNextRelation(l linkValue) (bool, error) {
	var rel *linkParam
	anchor := false
	for i := range l.params {
		switch l.params[i].name {
		case "rel":
			if rel != nil {
				return false, fmt.Errorf("link %q has two rel parameters", l.target)
			}
			rel = &l.params[i]
		case "anchor":
			anchor = true
		}
	}
	if rel == nil || !rel.hasValue {
		return false, nil
	}
	next := false
	for _, r := range strings.Fields(rel.value) {
		if strings.EqualFold(r, "next") {
			next = true
		}
	}
	if next && anchor {
		return false, fmt.Errorf("next link %q has an anchor parameter", l.target)
	}
	return next, nil
}

// parseLinkHeader parses one Link field value:
//
//	Link       = #link-value
//	link-value = "<" URI-Reference ">" *( OWS ";" OWS [ link-param ] )
//	link-param = token BWS [ "=" BWS ( token / quoted-string ) ]
//
// Empty list elements (",,") are accepted, as RFC 9110 requires of a
// recipient. Anything else that does not fit is an error.
func parseLinkHeader(v string) ([]linkValue, error) {
	p := &linkParser{s: v}
	var out []linkValue
	for {
		p.skipOWS()
		if p.eof() {
			return out, nil
		}
		if p.peek() == ',' { // an empty list element
			p.i++
			continue
		}
		l, err := p.linkValue()
		if err != nil {
			return nil, err
		}
		out = append(out, l)
		p.skipOWS()
		switch {
		case p.eof():
			return out, nil
		case p.peek() == ',':
			p.i++
		default:
			return nil, fmt.Errorf("unexpected %q at offset %d, after a link", p.peek(), p.i)
		}
	}
}

type linkParser struct {
	s string
	i int
}

func (p *linkParser) eof() bool  { return p.i >= len(p.s) }
func (p *linkParser) peek() byte { return p.s[p.i] }

func (p *linkParser) skipOWS() {
	for !p.eof() && (p.s[p.i] == ' ' || p.s[p.i] == '\t') {
		p.i++
	}
}

func (p *linkParser) linkValue() (linkValue, error) {
	var l linkValue
	if p.peek() != '<' {
		return l, fmt.Errorf("expected '<' at offset %d, found %q", p.i, p.peek())
	}
	end := strings.IndexByte(p.s[p.i+1:], '>')
	if end < 0 {
		return l, fmt.Errorf("the target at offset %d has no closing '>'", p.i)
	}
	l.target = p.s[p.i+1 : p.i+1+end]
	for j := 0; j < len(l.target); j++ {
		if c := l.target[j]; c <= ' ' || c == 0x7f || c == '<' {
			return l, fmt.Errorf("the target %q has a character a URI cannot have", l.target)
		}
	}
	p.i += end + 2
	for {
		p.skipOWS()
		if p.eof() || p.peek() != ';' {
			return l, nil
		}
		p.i++
		p.skipOWS()
		if p.eof() || p.peek() == ';' || p.peek() == ',' { // an empty parameter
			continue
		}
		param, err := p.param()
		if err != nil {
			return l, err
		}
		l.params = append(l.params, param)
	}
}

func (p *linkParser) param() (linkParam, error) {
	name, err := p.token()
	if err != nil {
		return linkParam{}, fmt.Errorf("parameter name at offset %d: %v", p.i, err)
	}
	param := linkParam{name: strings.ToLower(name)}
	p.skipOWS()
	if p.eof() || p.peek() != '=' {
		return param, nil
	}
	p.i++
	p.skipOWS()
	param.hasValue = true
	if !p.eof() && p.peek() == '"' {
		param.value, err = p.quoted()
	} else {
		param.value, err = p.token()
	}
	if err != nil {
		return param, fmt.Errorf("value of parameter %q: %v", param.name, err)
	}
	return param, nil
}

// isTokenChar is RFC 9110's tchar.
func isTokenChar(c byte) bool {
	switch {
	case c >= '0' && c <= '9', c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z':
		return true
	}
	return strings.IndexByte("!#$%&'*+-.^_`|~", c) >= 0
}

func (p *linkParser) token() (string, error) {
	start := p.i
	for !p.eof() && isTokenChar(p.peek()) {
		p.i++
	}
	if p.i == start {
		if p.eof() {
			return "", fmt.Errorf("missing")
		}
		return "", fmt.Errorf("unexpected %q", p.peek())
	}
	return p.s[start:p.i], nil
}

// quoted reads a quoted-string starting at the opening quote and returns its
// text with each quoted-pair resolved. Control characters other than a tab
// are not allowed inside.
func (p *linkParser) quoted() (string, error) {
	var b strings.Builder
	p.i++ // the opening quote
	for !p.eof() {
		c := p.peek()
		p.i++
		switch {
		case c == '"':
			return b.String(), nil
		case c == '\\':
			if p.eof() {
				return "", fmt.Errorf("a quoted string ends in a backslash")
			}
			c = p.peek()
			p.i++
			if c < ' ' && c != '\t' || c == 0x7f {
				return "", fmt.Errorf("a control character is escaped in a quoted string")
			}
			b.WriteByte(c)
		case c < ' ' && c != '\t', c == 0x7f:
			return "", fmt.Errorf("a control character inside a quoted string")
		default:
			b.WriteByte(c)
		}
	}
	return "", fmt.Errorf("a quoted string has no closing quote")
}
