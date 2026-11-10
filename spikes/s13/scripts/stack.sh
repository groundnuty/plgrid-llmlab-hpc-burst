#!/usr/bin/env bash
# The proxy stacks of S13's Envoy arms. s13fields -exec starts this in its own process group and
# stops the group; the stack serves on 127.0.0.1:29902 in the foreground.
#
#   stack.sh ring-hash <envoy binary>   Envoy alone, envoy/ring-hash.yaml
#   stack.sh llm-d                      llm-d's EPP behind Envoy 1.33.2: S2's "llmd-tuned" stack
#                                       (llm-d v1.0.0's no-Kubernetes guide, timeouts raised), built by
#                                       spikes/s2/scripts/setup.sh into $S2_HOME
#
# Environment: S13_WORK (set by s13fields) for generated configs and logs; S2_HOME for llm-d.
# Ports: Envoy 29902, its admin 29903, the EPP's gRPC 29904, health 29905 and metrics 29906; the
# recorders are the workers on 29900 and 29901.
set -euo pipefail
HERE=$(cd "$(dirname "$0")" && pwd)
WORK=${S13_WORK:?set S13_WORK}

case ${1:-} in
ring-hash)
  envoy=${2:?an Envoy binary}
  exec "$envoy" -c "$HERE/../envoy/ring-hash.yaml" --disable-hot-restart --concurrency 2 --log-level warn \
    --service-node s13-ring-hash
  ;;
llm-d)
  S2_HOME=${S2_HOME:?set S2_HOME to the directory spikes/s2/scripts/setup.sh filled}
  printf 'endpoints:\n' > "$WORK/endpoints.yaml"
  for n in 0 1; do
    printf '  - name: worker%d\n    address: 127.0.0.1\n    port: "%d"\n    labels:\n      model: s13-model\n' "$n" $((29900 + n)) >> "$WORK/endpoints.yaml"
  done
  # S2's generator: llm-d's shipped Envoy and EPP files, edited by exact-line substitution.
  python3 -I "$HERE/../../s2/scripts/genconf.py" "$S2_HOME/src" "$WORK" tuned guide \
    29902 29903 29904 29905 29906 "$WORK/endpoints.yaml" "$WORK/envoy-access.log" >/dev/null
  # The EPP's flags as S2's stack.sh starts it (spikes/s2/scripts/stack.sh, start_epp).
  "$S2_HOME/bin/epp" --config-file="$WORK/epp-config.yaml" --pool-name=file-discovery --pool-namespace=default \
    --grpc-port=29904 --grpc-health-port=29905 --metrics-port=29906 --secure-serving=false \
    --health-checking=true --tracing=false --v=2 > "$WORK/epp.log" 2>&1 &
  for _ in $(seq 1 120); do (exec 3<> /dev/tcp/127.0.0.1/29904) 2>/dev/null && break; sleep 0.5; done
  exec "$S2_HOME/bin/envoy-1.33.2" -c "$WORK/envoy.yaml" --disable-hot-restart --concurrency 2 --log-level warn \
    --drain-strategy immediate --drain-time-s 60 --service-node s13-llm-d
  ;;
*)
  echo "usage: stack.sh ring-hash <envoy> | llm-d" >&2; exit 64
  ;;
esac
