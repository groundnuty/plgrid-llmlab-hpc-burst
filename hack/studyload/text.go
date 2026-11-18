package main

// vocab is the fixed word list prompts are built from: 256 distinct lowercase English words, each a single token on the BPE tokenizers of
// the usual chat models (with its leading space), so that one word is about one token. A real tokenizer's count can still differ;
// usage.prompt_tokens, which every record carries, is the truth.
var vocab = [256]string{
	"apple", "river", "stone", "cloud", "green", "table", "paper", "light",
	"water", "heart", "music", "bridge", "garden", "window", "forest", "silver",
	"planet", "engine", "market", "castle", "ocean", "island", "valley", "winter",
	"summer", "autumn", "spring", "morning", "evening", "shadow", "mirror", "candle",
	"button", "ladder", "pocket", "basket", "blanket", "kitchen", "hammer", "needle",
	"thread", "feather", "pebble", "meadow", "harbor", "lantern", "compass", "anchor",
	"saddle", "barrel", "basin", "beacon", "blossom", "bottle", "bracelet", "branch",
	"breeze", "brook", "bucket", "cabin", "canyon", "carpet", "cellar", "chapel",
	"cherry", "circle", "clover", "cobweb", "copper", "cotton", "cradle", "crystal",
	"curtain", "dagger", "desert", "diamond", "dolphin", "dragon", "drawer", "dream",
	"eagle", "emerald", "falcon", "fabric", "fiddle", "fireplace", "flute", "fountain",
	"frost", "galaxy", "glacier", "goblet", "granite", "gravel", "hazel", "helmet",
	"hollow", "honey", "horizon", "jungle", "kettle", "kingdom", "lagoon", "lemon",
	"lilac", "marble", "meteor", "mineral", "moss", "nectar", "orchard", "oyster",
	"pearl", "pillow", "pirate", "plume", "prairie", "puzzle", "quartz", "rabbit",
	"raven", "ribbon", "rocket", "saffron", "sapphire", "scarlet", "shield", "sparrow",
	"spiral", "squirrel", "statue", "sunrise", "tavern", "temple", "thunder", "timber",
	"tunnel", "velvet", "violet", "voyage", "walnut", "whisper", "willow", "wizard",
	"zephyr", "anvil", "arrow", "badge", "baker", "banner", "beetle", "blade",
	"boulder", "brick", "butter", "cactus", "camel", "candy", "cannon", "canvas",
	"carrot", "chain", "chalk", "cheese", "chimney", "cliff", "clock", "coast",
	"comet", "cookie", "coral", "crown", "dancer", "dawn", "doctor", "dune",
	"ember", "field", "flame", "flower", "fossil", "garlic", "ginger", "glove",
	"grape", "harp", "hill", "hunter", "igloo", "jacket", "jewel", "kernel",
	"knight", "lamp", "leaf", "lizard", "magnet", "mango", "maple", "mason",
	"melon", "mitten", "mule", "oasis", "olive", "onion", "otter", "paddle",
	"panda", "parrot", "peach", "pepper", "piano", "pigeon", "pillar", "plank",
	"pond", "potato", "pumpkin", "quilt", "radish", "rain", "reef", "robin",
	"rope", "sailor", "salmon", "sand", "scroll", "shell", "shovel", "spider",
	"spoon", "stream", "sugar", "swan", "tiger", "tomato", "torch", "tower",
	"trail", "trumpet", "turtle", "umbrella", "vase", "wagon", "wheat", "wolf",
	"yarn", "zebra", "bamboo", "cedar", "dusk", "gate", "orbit", "quiver",
}

// words returns n space-separated words of the vocabulary, a function of seed alone: the same seed gives the same text, and two seeds
// differ from their first word with probability 255/256. A linear congruential walk picks each word, so a prompt is not a repeated cycle.
func words(n int, seed uint32) string {
	if n <= 0 {
		return ""
	}
	b := make([]byte, 0, n*7)
	state := seed
	for i := 0; i < n; i++ {
		state = state*1664525 + 1013904223
		if i > 0 {
			b = append(b, ' ')
		}
		b = append(b, vocab[state>>24]...)
	}
	return string(b)
}

// seedOf hashes a session ID and turn into a words seed (FNV-1a, 32 bits).
func seedOf(session string, turn int) uint32 {
	h := uint32(2166136261)
	for _, c := range []byte(session) {
		h = (h ^ uint32(c)) * 16777619
	}
	for _, c := range []byte{byte(turn), byte(turn >> 8)} {
		h = (h ^ uint32(c)) * 16777619
	}
	return h
}
