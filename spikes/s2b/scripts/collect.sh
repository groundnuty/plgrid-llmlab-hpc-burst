#!/usr/bin/env bash
# Copy the publishable part of a job's results from a raw results directory into the repository.
#
#   collect.sh RAW_DIR DEST_DIR
#
# RAW_DIR is the job's results directory (as job.sh writes it), already fetched from the cluster.
# Kept: per arm meta.json, client.out, reset.txt, ps.txt, listeners.txt, the client records and the
# 2 s samples (gzipped), and the before/after metrics reduced to the vllm: counters and gauges the
# analysis reads; per regime the placement logs (gzipped), kv-*.txt and the listener lists; env.txt.
# Not kept: worker, router, EPP and Envoy logs and configs (they carry paths and are large).
# The script refuses to finish unless pubcheck.py passes over everything it kept (private paths, account
# names and non-loopback addresses, in compressed contents too). Run pubcheck.py again on the whole diff
# before every push: it is the same check, applied to files collect.sh never saw.
set -euo pipefail
RAW=${1:?raw results dir}; DEST=${2:?destination}
mkdir -p "$DEST/arms"
KEEP_METRICS='^vllm:(prefix_cache_(hits|queries)_total|num_preemptions_total|request_success_total|kv_cache_usage_perc|num_requests_(running|waiting)|cache_config_info|prompt_tokens_total|generation_tokens_total)'
for a in "$RAW"/arms/*/; do
  n=$(basename "$a"); d=$DEST/arms/$n; mkdir -p "$d"
  for f in meta.json client.out reset.txt ps.txt SKIPPED; do [[ -f $a/$f ]] && cp "$a/$f" "$d/"; done
  for f in client.jsonl samples.tsv; do [[ -f $a/$f ]] && gzip -9 -c "$a/$f" > "$d/$f.gz"; done
  for f in "$a"/before-w*.prom "$a"/after-w*.prom; do [[ -f $f ]] && grep -E "$KEEP_METRICS" "$f" > "$d/$(basename "$f")" || true; done
done
for f in "$RAW"/placement-*.tsv; do [[ -f $f ]] && gzip -9 -c "$f" > "$DEST/$(basename "$f").gz"; done
for f in "$RAW"/kv-*.txt "$RAW"/env.txt "$RAW"/failures.txt; do [[ -f $f ]] && cp "$f" "$DEST/"; done
# Listener lists: keep loopback addresses; any other IPv4 address (the node's) becomes <node-address>.
scrub() { sed -E 's/\b(25[0-5]|2[0-4][0-9]|1?[0-9]?[0-9])(\.(25[0-5]|2[0-4][0-9]|1?[0-9]?[0-9])){3}\b/<ip>/g; s/<ip>:/<node-address>:/g' "$1" | sed -E 's/^(.*) <node-address>:/\1 <node-address>:/'; }
for f in "$RAW"/listeners-*.txt "$RAW"/arms/*/listeners.txt; do
  [[ -f $f ]] || continue
  rel=${f#"$RAW"/}; mkdir -p "$DEST/$(dirname "$rel")"
  sed -E 's/(^| )127\.0\.0\.1:/\1LOOPBACK4:/' "$f" | scrub /dev/stdin | sed 's/LOOPBACK4:/127.0.0.1:/' > "$DEST/$rel"
done
python3 -I -B "$(dirname "$0")/pubcheck.py" --ips "$DEST" || { echo "collect: the publication check refused $DEST" >&2; exit 1; }
echo "collected into $DEST"
