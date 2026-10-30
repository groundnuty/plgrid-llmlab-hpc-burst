# Sourced by setup.sh and test-setup.sh. Build stamps for the patched upstream binaries.
#
# A binary is current only if a stamp next to it names the source commit, the patch file and the
# patched tree it was built from. A binary with no stamp, or a stale one, is rebuilt: an executable
# left in S2_HOME by an earlier setup, for example one built before the loopback patch existed,
# is never trusted just because it exists.

# stamp_of <srcdir> <patchfile>: the identity a binary built from this tree would have
stamp_of() {
  printf 'commit=%s patch=%s tree=%s' \
    "$(git -C "$1" rev-parse HEAD)" \
    "$(sha256sum < "$2" | cut -d' ' -f1)" \
    "$(git -C "$1" diff HEAD | sha256sum | cut -d' ' -f1)"
}

# build_patched <srcdir> <patchfile> <binary> <go package>
# Rebuilds unless the stamp matches. The stamp is removed before a build and written only after it
# succeeded, so a failed build never leaves a stamp for a binary it did not produce.
build_patched() {
  local dir=$1 patch=$2 bin=$3 pkg=$4 want
  want=$(stamp_of "$dir" "$patch") || return 1
  if [[ -x $bin && -f $bin.stamp && $(cat "$bin.stamp") == "$want" ]]; then
    echo "$(basename "$bin"): current ($want)"
    return 0
  fi
  echo "$(basename "$bin"): building (no matching build stamp)"
  rm -f "$bin.stamp" "$bin.new"
  (cd "$dir" && nice -n 10 go build -o "$bin.new" "$pkg") || { rm -f "$bin.new"; return 1; }
  mv "$bin.new" "$bin" && echo "$want" > "$bin.stamp"
}
