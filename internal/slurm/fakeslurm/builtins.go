package fakeslurm

// New. The file and environment commands of the grammar: cat >, printenv,
// sha256sum, umask, mkdir -p, chmod, mv -f and mv -fT, rm -f, rm -rf and test -f, over
// the platform's in-memory file system.

import (
	"crypto/sha256"
	"encoding/hex"
	"io/fs"
	"path"
	"strconv"
	"strings"
)

func cleanPath(p string) string { return path.Clean(p) }

// cat is "cat > <file>": standard input to the file, mode 0666 &^ umask. An
// existing file keeps its mode, as a shell redirection does.
func (sh *shell) cat(c simpleCmd) int {
	if c.words[0] != "cat" || len(c.words) != 1 {
		return sh.fail(2, "bash: line 1: the fake supports only: cat > <file>")
	}
	name := sh.path(c.redirect)
	fsys := &sh.p.fs
	switch {
	case fsys.isDir(name):
		return sh.fail(1, "bash: line 1: %s: Is a directory", name)
	case !fsys.isDir(path.Dir(name)):
		return sh.fail(1, "bash: line 1: %s: No such file or directory", name)
	}
	content := append([]byte(nil), sh.stdin...)
	if f := fsys.files[name]; f != nil {
		f.Content = content
		return 0
	}
	fsys.files[name] = &File{Content: content, Mode: sh.fileMode(), Owner: sh.s.user}
	return 0
}

// printenv is "printenv <VAR>": the value and a newline, or exit 1 when unset.
func (sh *shell) printenv(args []string) int {
	if len(args) != 1 {
		return sh.fail(2, "printenv: the fake supports only: printenv <VAR>")
	}
	v, ok := sh.s.env[args[0]]
	if !ok {
		return 1
	}
	sh.out("%s\n", v)
	return 0
}

// sha256sum is "sha256sum <path>": "<hex>  <path>", exit 1 when missing.
func (sh *shell) sha256sum(args []string) int {
	if len(args) != 1 {
		return sh.fail(2, "sha256sum: the fake supports only: sha256sum <path>")
	}
	f := sh.p.fs.files[sh.path(args[0])]
	if f == nil {
		return sh.fail(1, "sha256sum: %s: No such file or directory", args[0])
	}
	sum := sha256.Sum256(f.Content)
	sh.out("%s  %s\n", hex.EncodeToString(sum[:]), args[0])
	return 0
}

// setUmask is "umask <octal>": it holds for the rest of the line.
func (sh *shell) setUmask(args []string) int {
	if len(args) != 1 {
		return sh.fail(2, "umask: the fake supports only: umask <octal>")
	}
	m, err := strconv.ParseUint(args[0], 8, 32)
	if err != nil || m > 0o777 {
		return sh.fail(1, "bash: line 1: umask: %s: octal number out of range", args[0])
	}
	sh.umask = fs.FileMode(m)
	return 0
}

// mkdir is "mkdir -p <dir>": missing directories get mode 0777 &^ umask.
func (sh *shell) mkdir(args []string) int {
	if len(args) != 2 || args[0] != "-p" {
		return sh.fail(2, "mkdir: the fake supports only: mkdir -p <dir>")
	}
	if err := sh.p.fs.mkdirAll(sh.path(args[1]), sh.dirMode()); err != nil {
		return sh.fail(1, "mkdir: %v", err)
	}
	return 0
}

// chmod is "chmod <octal> <file>"; a directory works too.
func (sh *shell) chmod(args []string) int {
	if len(args) != 2 {
		return sh.fail(2, "chmod: the fake supports only: chmod <octal> <file>")
	}
	m, err := strconv.ParseUint(args[0], 8, 32)
	if err != nil || m > 0o7777 {
		return sh.fail(1, "chmod: invalid mode: '%s'", args[0])
	}
	name := sh.path(args[1])
	fsys := &sh.p.fs
	switch {
	case fsys.files[name] != nil:
		fsys.files[name].Mode = fs.FileMode(m).Perm()
	case fsys.isDir(name):
		fsys.dirs[name] = fs.FileMode(m).Perm()
	default:
		return sh.fail(1, "chmod: cannot access '%s': No such file or directory", args[1])
	}
	return 0
}

// mv is "mv -f <a> <b>" or "mv -fT <a> <b>" for a file, as GNU mv does it
// (mv_real_test.go compares the two on the real mv): it replaces b and keeps the
// file's mode and owner. A destination that is a directory takes the file
// inside it under its own name, and succeeds, unless -T says b is the file's
// own path, which fails ("cannot overwrite directory"). The plugin installs with
// -fT, so a directory at the path fails the install instead of swallowing the
// file.
func (sh *shell) mv(args []string) int {
	force, noTarget := false, false
	for len(args) > 0 && strings.HasPrefix(args[0], "-") && len(args[0]) > 1 {
		for _, c := range args[0][1:] {
			switch c {
			case 'f':
				force = true
			case 'T':
				noTarget = true
			default:
				return sh.fail(2, "mv: the fake supports only: mv -f|-fT <a> <b>")
			}
		}
		args = args[1:]
	}
	if !force || len(args) != 2 {
		return sh.fail(2, "mv: the fake supports only: mv -f|-fT <a> <b>")
	}
	from, to, toArg := sh.path(args[0]), sh.path(args[1]), args[1]
	fsys := &sh.p.fs
	f := fsys.files[from]
	if f == nil {
		return sh.fail(1, "mv: cannot stat '%s': No such file or directory", args[0])
	}
	if fsys.isDir(to) {
		if noTarget {
			return sh.fail(1, "mv: cannot overwrite directory '%s' with non-directory", toArg)
		}
		to, toArg = path.Join(to, path.Base(from)), path.Join(toArg, path.Base(from))
		if fsys.isDir(to) {
			return sh.fail(1, "mv: cannot overwrite directory '%s' with non-directory", toArg)
		}
	}
	if !fsys.isDir(path.Dir(to)) {
		return sh.fail(1, "mv: cannot move '%s' to '%s': No such file or directory", args[0], toArg)
	}
	delete(fsys.files, from)
	fsys.files[to] = f
	return 0
}

// rm is "rm -f <file>" (a directory is refused) or "rm -rf <path>". A missing
// path is not an error.
func (sh *shell) rm(args []string) int {
	if len(args) != 2 || (args[0] != "-f" && args[0] != "-rf") {
		return sh.fail(2, "rm: the fake supports only: rm -f <file> and rm -rf <path>")
	}
	name := sh.path(args[1])
	fsys := &sh.p.fs
	if args[0] == "-f" && fsys.isDir(name) {
		return sh.fail(1, "rm: cannot remove '%s': Is a directory", args[1])
	}
	fsys.removeTree(name)
	return 0
}

// test is "test -f <path>": exit 0 for a file, 1 otherwise.
func (sh *shell) test(args []string) int {
	if len(args) != 2 || args[0] != "-f" {
		return sh.fail(2, "test: the fake supports only: test -f <path>")
	}
	if sh.p.fs.isFile(sh.path(args[1])) {
		return 0
	}
	return 1
}
