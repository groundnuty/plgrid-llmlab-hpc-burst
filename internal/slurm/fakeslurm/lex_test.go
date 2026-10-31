package fakeslurm

// New. The expected words are written out by hand from the grammar in doc.go.

import (
	"reflect"
	"strings"
	"testing"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/slurm"
)

func TestParseLine(t *testing.T) {
	const wrap = "--wrap=[ $(sha256sum /x | cut -c1-64) = abc ] && exec /x /y"
	for _, tc := range []struct {
		name string
		line string
		want []simpleCmd
	}{
		{"empty line", "", nil},
		{"one command", "sbatch --parsable", []simpleCmd{{words: []string{"sbatch", "--parsable"}}}},
		{"and chain with a redirection", "umask 077 && mkdir -p /a/b && cat > /a/b/f",
			[]simpleCmd{
				{words: []string{"umask", "077"}},
				{words: []string{"mkdir", "-p", "/a/b"}},
				{words: []string{"cat"}, redirect: "/a/b/f", redirected: true},
			}},
		{"quotes hide && and the shell syntax inside them", "sbatch '" + wrap + "' --job-name=a",
			[]simpleCmd{{words: []string{"sbatch", wrap, "--job-name=a"}}}},
		{"a quoted && is a word", "echo '&&' b", []simpleCmd{{words: []string{"echo", "&&", "b"}}}},
		{"an empty quoted word", "x '' y", []simpleCmd{{words: []string{"x", "", "y"}}}},
		{"quotes may touch plain text", "x --a='b c'", []simpleCmd{{words: []string{"x", "--a=b c"}}}},
		{"a quoted > is a word", "echo '>' f", []simpleCmd{{words: []string{"echo", ">", "f"}}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := parseLine(tc.line)
			if err != nil {
				t.Fatalf("parseLine(%q): %v", tc.line, err)
			}
			if !reflect.DeepEqual(got, tc.want) {
				t.Errorf("parseLine(%q)\n got  %+v\n want %+v", tc.line, got, tc.want)
			}
		})
	}
}

// Anything outside the grammar is a syntax error, so a dialect that leaks shell
// syntax fails a test instead of passing unnoticed.
func TestParseLineRefusesShellSyntax(t *testing.T) {
	for _, line := range []string{
		"a; b", "a | b", "a || b", "a & b", "a &&b", "a&& b", "echo $HOME", "echo ~/x", "echo $(id)",
		"echo `id`", `echo "x"`, `echo \x`, "cat >f", "cat> f", "cat >> f", "cat < f", "a 2> f",
		"cat >", "cat > f g", "cat > f > g", "> f", "a &&", "&& a", "a && && b", "a\nb", "a\tb",
		"x 'unterminated", "echo *.go", "echo {a,b}", "echo a#b", "echo (x)",
	} {
		if _, err := parseLine(line); err == nil {
			t.Errorf("parseLine(%q) succeeded, want a syntax error", line)
		}
	}
}

// slurm.Line is what the plugin uses to build every line. Whatever it builds,
// the fake reads back as the same words.
func TestParseLineReadsWhatSlurmLineBuilds(t *testing.T) {
	for _, words := range [][]string{
		{"sbatch", "--parsable", "--job-name=hb-0123456789ab", "--time=30"},
		{"sbatch", "--wrap=[ $(sha256sum /x | cut -c1-64) = abc ] && exec /x /y", "--hold"},
		{"mkdir", "-p", "/scratch/example-user/hpcburst"},
		{"printenv", "HOME"},
		{"x", "with space", "a@b%c+d", "--k=v w"},
	} {
		line, err := slurm.Line(words...)
		if err != nil {
			t.Fatal(err)
		}
		got, err := parseLine(line)
		if err != nil || len(got) != 1 || !reflect.DeepEqual(got[0].words, words) {
			t.Errorf("parseLine(%q) = %+v, %v, want the words %q", line, got, err, strings.Join(words, "|"))
		}
	}
}

func TestSimpleCmdCallWords(t *testing.T) {
	c := simpleCmd{words: []string{"cat"}, redirect: "/f", redirected: true}
	if got, want := c.callWords(), []string{"cat", ">", "/f"}; !reflect.DeepEqual(got, want) {
		t.Errorf("callWords = %v, want %v", got, want)
	}
	if len(c.words) != 1 {
		t.Errorf("callWords changed the command's own words: %v", c.words)
	}
}
