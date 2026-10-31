package enginedouble

import (
	"encoding/json"
	"hash/fnv"
	"strconv"
	"strings"
)

// The tokenizer: a token is a whitespace-separated word, and its id is the
// FNV-1a 32-bit hash of the word, modulo 100000, plus 1.

// wordID is the token id of one word.
func wordID(w string) int32 {
	h := fnv.New32a()
	_, _ = h.Write([]byte(w))
	return int32(h.Sum32()%100000) + 1
}

// tokenize splits text into tokens: the words and their ids.
func tokenize(text string) (words []string, ids []int32) {
	words = strings.Fields(text)
	ids = make([]int32, len(words))
	for i, w := range words {
		ids[i] = wordID(w)
	}
	return words, ids
}

// vocabSize is the size of the vocabulary generated tokens come from.
const vocabSize = 1024

// vocab is the fixed vocabulary of generated tokens: single words whose ids
// are all distinct, so a generated token is told from another by its id. It
// is built from syllables in a fixed order, skipping a word whose id an
// earlier word took.
var vocab = buildVocab()

func buildVocab() []string {
	onsets := []string{"b", "d", "f", "g", "h", "j", "k", "l", "m", "n", "p", "r", "s", "t", "v", "w", "z", "br", "dr", "st"}
	vowels := []string{"a", "e", "i", "o", "u"}
	codas := []string{"b", "d", "g", "k", "l", "m", "n", "p", "r", "s", "t", "x", "z"}
	words := make([]string, 0, vocabSize)
	used := map[int32]bool{}
	for _, o := range onsets {
		for _, v := range vowels {
			for _, c := range codas {
				w := o + v + c
				if id := wordID(w); !used[id] && len(words) < vocabSize {
					used[id] = true
					words = append(words, w)
				}
			}
		}
	}
	if len(words) != vocabSize {
		panic("enginedouble: the vocabulary generator made " + strconv.Itoa(len(words)) + " words")
	}
	return words
}

// message is one chat message as the double reads it.
type message struct {
	Role    string          `json:"role"`
	Content json.RawMessage `json:"content"`
}

// text returns the message's text: a string content as it is, and the text
// parts of a list content joined by a space. A null or missing content is "".
func (m message) text() string {
	if len(m.Content) == 0 {
		return ""
	}
	var s string
	if json.Unmarshal(m.Content, &s) == nil {
		return s
	}
	var parts []struct {
		Type string `json:"type"`
		Text string `json:"text"`
	}
	if json.Unmarshal(m.Content, &parts) != nil {
		return ""
	}
	var texts []string
	for _, p := range parts {
		if p.Type == "text" || p.Type == "" {
			texts = append(texts, p.Text)
		}
	}
	return strings.Join(texts, " ")
}

// renderChat renders a conversation with the double's chat template: each
// message as "<|role|> " + content + " <|assistant|>", then, when
// addGenerationPrompt is set, the generation prompt of overhead-2 words. One
// empty user message with the generation prompt is exactly overhead tokens.
func renderChat(msgs []message, addGenerationPrompt bool, overhead int) string {
	var b strings.Builder
	for _, m := range msgs {
		b.WriteString("<|" + m.Role + "|> " + m.text() + " <|assistant|>\n")
	}
	if addGenerationPrompt {
		for i := 0; i < overhead-2; i++ {
			b.WriteString("<|gen" + strconv.Itoa(i) + "|> ")
		}
	}
	return b.String()
}
