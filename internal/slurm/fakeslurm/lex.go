package fakeslurm

// New. The reader of the command grammar in doc.go: a quote-aware tokenizer
// and a parser for "simple && simple" lines with one "cat > file" redirection.

import (
	"fmt"
	"strings"
)

type tokKind int

const (
	tokWord tokKind = iota
	tokAnd
	tokRedirect
)

type token struct {
	kind tokKind
	text string
}

// simpleCmd is one simple command. For "cat > f", words is ["cat"] and
// redirect is f.
type simpleCmd struct {
	words      []string
	redirect   string
	redirected bool
}

// callWords are the words as the call log shows them: the redirection is part
// of the command.
func (c simpleCmd) callWords() []string {
	w := append([]string(nil), c.words...)
	if c.redirected {
		w = append(w, ">", c.redirect)
	}
	return w
}

// isPlain reports whether c may appear in a word outside single quotes: the
// plain set of slurm.shellWord.
func isPlain(c byte) bool {
	switch {
	case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9':
		return true
	}
	return strings.IndexByte("._:=,/@%+-", c) >= 0
}

// opEnds reports whether an operator that ends just before index i is a whole
// word: followed by a space or the end of the line.
func opEnds(line string, i int) bool { return i >= len(line) || line[i] == ' ' }

// tokenize splits a line into words and the operators "&&" and ">". An
// operator counts only when it stands alone and is not quoted.
func tokenize(line string) ([]token, error) {
	var toks []token
	var cur strings.Builder
	inWord, inQuote := false, false
	flush := func() {
		if inWord {
			toks = append(toks, token{kind: tokWord, text: cur.String()})
			cur.Reset()
			inWord = false
		}
	}
	for i := 0; i < len(line); i++ {
		c := line[i]
		switch {
		case inQuote:
			if c == '\'' {
				inQuote = false
			} else {
				cur.WriteByte(c)
			}
		case c == '\'':
			inQuote, inWord = true, true
		case c == ' ':
			flush()
		case isPlain(c):
			cur.WriteByte(c)
			inWord = true
		case c == '&' && !inWord && strings.HasPrefix(line[i:], "&&") && opEnds(line, i+2):
			toks = append(toks, token{kind: tokAnd})
			i++
		case c == '>' && !inWord && opEnds(line, i+1):
			toks = append(toks, token{kind: tokRedirect})
		default:
			return nil, fmt.Errorf("syntax error near unexpected token `%c'", c)
		}
	}
	if inQuote {
		return nil, fmt.Errorf("unexpected EOF while looking for matching `''")
	}
	flush()
	return toks, nil
}

// parseLine reads a line into simple commands. An empty line is no commands.
func parseLine(line string) ([]simpleCmd, error) {
	toks, err := tokenize(line)
	if err != nil {
		return nil, err
	}
	var cmds []simpleCmd
	var cur simpleCmd
	finish := func() error {
		if len(cur.words) == 0 {
			return fmt.Errorf("syntax error near unexpected token `&&'")
		}
		cmds = append(cmds, cur)
		cur = simpleCmd{}
		return nil
	}
	for i := 0; i < len(toks); i++ {
		switch t := toks[i]; t.kind {
		case tokWord:
			if cur.redirected {
				return nil, fmt.Errorf("syntax error: a word after the redirection target")
			}
			cur.words = append(cur.words, t.text)
		case tokRedirect:
			if i+1 >= len(toks) || toks[i+1].kind != tokWord || cur.redirected {
				return nil, fmt.Errorf("syntax error near unexpected token `>'")
			}
			cur.redirect, cur.redirected = toks[i+1].text, true
			i++
		case tokAnd:
			if err := finish(); err != nil {
				return nil, err
			}
		}
	}
	if len(toks) == 0 {
		return nil, nil
	}
	if err := finish(); err != nil {
		return nil, fmt.Errorf("syntax error: the line ends after `&&' or `>'")
	}
	return cmds, nil
}
