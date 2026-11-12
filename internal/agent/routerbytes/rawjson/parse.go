// Copied from spikes/s13/rawjson/parse.go (spike S13, PR #324), byte for byte below the next line, with
// only S13's import paths mapped to this directory: #429's Bytes test needs S13's cases, its raw JSON
// comparison and its recorder in the root module (team-lead's answer T4). drift_test.go checks the copy.
// End of provenance.

// Package rawjson parses JSON into a tree that keeps, for every value, the exact bytes it was
// written with, and compares two bodies path by path.
//
// It exists so that a comparison never goes through a decoded model: a number keeps its literal
// text (1, 1.0 and 1e0 stay distinct, and an int64 never passes through a float64), a string keeps
// its escapes, and an object keeps its members in the order they were written. Decoding happens
// only where a comparison asks whether two different spellings denote the same value.
package rawjson

import (
	"encoding/json"
	"fmt"
)

// Kind is a JSON value's type.
type Kind int

const (
	Null Kind = iota
	Bool
	Number
	String
	Array
	Object
)

func (k Kind) String() string {
	return [...]string{"null", "bool", "number", "string", "array", "object"}[k]
}

// Node is one JSON value. Raw is the value's exact bytes in the input, with no surrounding
// whitespace. Members (objects) keep the input's order; Elems (arrays) likewise.
type Node struct {
	Kind    Kind
	Raw     []byte
	Members []Member
	Elems   []*Node
}

// Member is one object member. Key is the decoded key; KeyRaw is its literal, quotes included.
type Member struct {
	Key    string
	KeyRaw []byte
	Value  *Node
}

// Get returns the value of the member named key, or nil. With duplicate keys it returns the
// last, as Python's json module and pydantic do.
func (n *Node) Get(key string) *Node {
	var v *Node
	for _, m := range n.Members {
		if m.Key == key {
			v = m.Value
		}
	}
	return v
}

// Parse parses one JSON value; whitespace may surround it. It refuses what encoding/json refuses.
func Parse(b []byte) (*Node, error) {
	if !json.Valid(b) {
		return nil, fmt.Errorf("not valid JSON")
	}
	p := &parser{b: b}
	p.ws()
	n, err := p.value()
	if err != nil {
		return nil, err
	}
	p.ws()
	if p.i != len(p.b) {
		return nil, fmt.Errorf("trailing bytes at offset %d", p.i)
	}
	return n, nil
}

type parser struct {
	b []byte
	i int
}

func (p *parser) ws() {
	for p.i < len(p.b) {
		switch p.b[p.i] {
		case ' ', '\t', '\n', '\r':
			p.i++
		default:
			return
		}
	}
}

func (p *parser) value() (*Node, error) {
	if p.i >= len(p.b) {
		return nil, fmt.Errorf("unexpected end at offset %d", p.i)
	}
	start := p.i
	switch c := p.b[p.i]; {
	case c == '{':
		return p.object()
	case c == '[':
		return p.array()
	case c == '"':
		if err := p.str(); err != nil {
			return nil, err
		}
		return &Node{Kind: String, Raw: p.b[start:p.i]}, nil
	case c == 't':
		return p.lit("true", Bool)
	case c == 'f':
		return p.lit("false", Bool)
	case c == 'n':
		return p.lit("null", Null)
	default:
		for p.i < len(p.b) && isNumByte(p.b[p.i]) {
			p.i++
		}
		if p.i == start {
			return nil, fmt.Errorf("unexpected byte %q at offset %d", c, start)
		}
		return &Node{Kind: Number, Raw: p.b[start:p.i]}, nil
	}
}

func isNumByte(c byte) bool {
	return c == '-' || c == '+' || c == '.' || c == 'e' || c == 'E' || (c >= '0' && c <= '9')
}

func (p *parser) lit(s string, k Kind) (*Node, error) {
	if len(p.b)-p.i < len(s) || string(p.b[p.i:p.i+len(s)]) != s {
		return nil, fmt.Errorf("bad literal at offset %d", p.i)
	}
	start := p.i
	p.i += len(s)
	return &Node{Kind: k, Raw: p.b[start:p.i]}, nil
}

// str advances past one string literal; json.Valid has already checked its escapes.
func (p *parser) str() error {
	p.i++ // opening quote
	for p.i < len(p.b) {
		switch p.b[p.i] {
		case '\\':
			p.i += 2
		case '"':
			p.i++
			return nil
		default:
			p.i++
		}
	}
	return fmt.Errorf("unterminated string")
}

func (p *parser) object() (*Node, error) {
	start := p.i
	p.i++
	n := &Node{Kind: Object}
	p.ws()
	if p.b[p.i] == '}' {
		p.i++
		n.Raw = p.b[start:p.i]
		return n, nil
	}
	for {
		p.ws()
		ks := p.i
		if err := p.str(); err != nil {
			return nil, err
		}
		keyRaw := p.b[ks:p.i]
		var key string
		if err := json.Unmarshal(keyRaw, &key); err != nil {
			return nil, err
		}
		p.ws()
		p.i++ // colon
		p.ws()
		v, err := p.value()
		if err != nil {
			return nil, err
		}
		n.Members = append(n.Members, Member{Key: key, KeyRaw: keyRaw, Value: v})
		p.ws()
		if p.b[p.i] == ',' {
			p.i++
			continue
		}
		p.i++ // closing brace
		n.Raw = p.b[start:p.i]
		return n, nil
	}
}

func (p *parser) array() (*Node, error) {
	start := p.i
	p.i++
	n := &Node{Kind: Array}
	p.ws()
	if p.b[p.i] == ']' {
		p.i++
		n.Raw = p.b[start:p.i]
		return n, nil
	}
	for {
		p.ws()
		v, err := p.value()
		if err != nil {
			return nil, err
		}
		n.Elems = append(n.Elems, v)
		p.ws()
		if p.b[p.i] == ',' {
			p.i++
			continue
		}
		p.i++ // closing bracket
		n.Raw = p.b[start:p.i]
		return n, nil
	}
}
