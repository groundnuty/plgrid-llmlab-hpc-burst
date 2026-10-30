#!/usr/bin/env python3
"""Fill the __NAME__ placeholders of a template and print the result.

    render_tmpl.py TEMPLATE KEY=VALUE [KEY=VALUE ...]  > out

Fails (exit 1, nothing printed) when the template still has an unfilled
__NAME__ placeholder, or when a KEY is not used by the template. That keeps a
typo from turning into a silently wrong config.

Platform lines: a line that starts with "#@<platform>[,<platform>...] " is kept,
with that prefix removed, only when PLATFORM is one of the listed names; otherwise
the line is dropped. The job template uses it for "#@athena,helios-gh200 #SBATCH
--gres=gpu:1", because #SBATCH
lines cannot expand variables. Left unrendered, such a line is a plain comment.
Python 3.6 compatible.
"""
import re
import sys

PLACEHOLDER = re.compile(r"__([A-Z][A-Z0-9_]*)__")
PLATFORM_LINE = re.compile(r"^#@([a-z][a-z0-9_-]*(?:,[a-z][a-z0-9_-]*)*) (.*)$")


def select_platform_lines(text, platform):
    out = []
    for line in text.splitlines(True):
        match = PLATFORM_LINE.match(line.rstrip("\n"))
        if match is None:
            out.append(line)
        elif platform is None:
            raise ValueError("template has #@<platform> lines but PLATFORM=... was not given")
        elif platform in match.group(1).split(","):
            out.append(match.group(2) + ("\n" if line.endswith("\n") else ""))
    return "".join(out)


def render(text, values):
    used = set()
    text = select_platform_lines(text, values.get("PLATFORM"))

    def sub(match):
        name = match.group(1)
        if name not in values:
            return match.group(0)
        used.add(name)
        return values[name]

    out = PLACEHOLDER.sub(sub, text)
    left = sorted(set(PLACEHOLDER.findall(out)))
    unused = sorted(set(values) - used)
    return out, left, unused


def main(argv):
    if len(argv) < 2:
        sys.stderr.write("usage: render_tmpl.py TEMPLATE KEY=VALUE ...\n")
        return 2
    values = {}
    for item in argv[2:]:
        key, sep, val = item.partition("=")
        if not sep or not key:
            sys.stderr.write("render_tmpl: bad argument %r, want KEY=VALUE\n" % item)
            return 2
        values[key] = val
    with open(argv[1]) as fh:
        text = fh.read()
    try:
        out, left, unused = render(text, values)
    except ValueError as exc:
        sys.stderr.write("render_tmpl: %s\n" % exc)
        return 1
    if left:
        sys.stderr.write("render_tmpl: unfilled placeholders in %s: %s\n" % (argv[1], ", ".join(left)))
        return 1
    if unused:
        sys.stderr.write("render_tmpl: keys not used by %s: %s\n" % (argv[1], ", ".join(unused)))
        return 1
    sys.stdout.write(out)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
