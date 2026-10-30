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
# The script refuses to finish if a kept file contains a path under /net/, /home/ or /tmp/, or "plg".
set -euo pipefail
RAW=${1:?raw results dir}; DEST=${2:?destination}
mkdir -p "$DEST/arms"
KEEP_METRICS='^vllm:(prefix_cache_(hits|queries)_total|num_preemptions_total|request_success_total|kv_cache_usage_perc|num_requests_(running|waiting)|cache_config_info|prompt_tokens_total|generation_tokens_total)'
for a in "$RAW"/arms/*/; do
  n=$(basename "$a"); d=$DEST/arms/$n; mkdir -p "$d"
  for f in meta.json client.out reset.txt ps.txt listeners.txt SKIPPED; do [[ -f $a/$f ]] && cp "$a/$f" "$d/"; done
  for f in client.jsonl samples.tsv; do [[ -f $a/$f ]] && gzip -9 -c "$a/$f" > "$d/$f.gz"; done
  for f in "$a"/before-w*.prom "$a"/after-w*.prom; do [[ -f $f ]] && grep -E "$KEEP_METRICS" "$f" > "$d/$(basename "$f")" || true; done
done
for f in "$RAW"/placement-*.tsv; do [[ -f $f ]] && gzip -9 -c "$f" > "$DEST/$(basename "$f").gz"; done
for f in "$RAW"/kv-*.txt "$RAW"/listeners-*.txt "$RAW"/env.txt; do [[ -f $f ]] && cp "$f" "$DEST/"; done
if grep -r -l -E '/net/|/home/|/tmp/|plg' "$DEST" --exclude='*.gz' ; then echo "collect: a kept file names a path or account (above)" >&2; exit 1; fi
for g in $(find "$DEST" -name '*.gz'); do if zcat "$g" | grep -q -E '/net/|/home/|/tmp/|plg'; then echo "collect: $g names a path or account" >&2; exit 1; fi; done
echo "collected into $DEST"
