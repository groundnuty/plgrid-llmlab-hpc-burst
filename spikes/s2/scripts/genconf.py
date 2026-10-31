#!/usr/bin/env python3
"""Generate the Envoy and EPP configs of one llm-d stack from the files llm-d ships.

The shipped guide files (llm-d v1.0.0, guides/no-kubernetes-deployment/router/) are read from the
pinned clone and edited by exact-line substitution; the script fails if a line it expects is not
there, so a changed upstream file cannot silently produce an unintended config. Nothing from the
upstream files is copied into this repository.

usage: genconf.py SRC_ROOT OUT_DIR ENVOY_VARIANT EPP_VARIANT ENVOY_PORT ADMIN_PORT EPP_GRPC EPP_HEALTH EPP_METRICS ENDPOINTS_FILE ACCESS_LOG
  ENVOY_VARIANT: shipped | tuned | probe
  EPP_VARIANT:   guide | session
"""
import os
import pathlib
import sys

(src, out, env_variant, epp_variant, envoy_port, admin_port, epp_grpc, _epp_health, _epp_metrics,
 endpoints, access_log) = sys.argv[1:12]
guide = pathlib.Path(src) / "llm-d/guides/no-kubernetes-deployment/router"
out = pathlib.Path(out)
out.mkdir(parents=True, exist_ok=True)


def sub(text, old, new, count=1):
    n = text.count(old)
    if n != count:
        sys.exit(f"genconf: expected {count} occurrence(s) of {old!r}, found {n}")
    return text.replace(old, new)


# ---- Envoy ----
t = (guide / "envoy/envoy.yaml").read_text()
t = sub(t, "address: 0.0.0.0\n", "address: 127.0.0.1\n")            # loopback only
t = sub(t, "port_value: 8081\n", f"port_value: {envoy_port}\n")
t = sub(t, "port_value: 19000\n", f"port_value: {admin_port}\n")
t = sub(t, "port_value: 9002\n", f"port_value: {epp_grpc}\n")
t = sub(t, "authority: localhost:9002", f"authority: localhost:{epp_grpc}")
t = sub(t, "stat_prefix: http-8081\n",
        "stat_prefix: http-8081\n"
        "                access_log:\n"
        "                  - name: envoy.access_loggers.file\n"
        "                    typed_config:\n"
        '                      "@type": type.googleapis.com/envoy.extensions.access_loggers.file.v3.FileAccessLog\n'
        f"                      path: {access_log}\n"
        "                      log_format:\n"
        "                        text_format_source:\n"
        '                          inline_string: "%START_TIME% sess=%REQ(x-session-id)% upstream=%UPSTREAM_HOST% code=%RESPONSE_CODE% flags=%RESPONSE_FLAGS% details=%RESPONSE_CODE_DETAILS% dur_ms=%DURATION% bytes=%BYTES_SENT%\\n"\n')
route_to = "timeout: 86400s\n                            idle_timeout: 86400s\n"
assert route_to in t, "route timeout lines not found"
if env_variant == "shipped":
    pass
elif env_variant == "tuned":
    # Route total and stream idle raised above the longest stream (3 h; the route timeout is a total deadline).
    t = sub(t, route_to, "timeout: 10800s\n                            idle_timeout: 10800s\n")
    # Envoy validates message_timeout inside [0 s, 1 h]; 3600 s is the largest it accepts.
    t = sub(t, "message_timeout: 1000s", "message_timeout: 3600s")
elif env_variant == "probe":
    # Deliberately tiny ext_proc timers, to show whether they bound a stream under FULL_DUPLEX_STREAMED.
    t = sub(t, "message_timeout: 1000s", "message_timeout: 5s")
    t = sub(t, "                        timeout: 10s\n", "                        timeout: 5s\n")
else:
    sys.exit("unknown envoy variant " + env_variant)
(out / "envoy.yaml").write_text(t)

# ---- EPP ----
e = (guide / "epp/config.yaml").read_text()
e = sub(e, "path: /etc/epp/endpoints.yaml", f"path: {endpoints}")
# The shipped config scrapes the workers every 50 ms. Setting EPP_SCRAPE_INTERVAL (for example 500ms)
# lowers the CPU cost on a shared host, but any value above the utilization detector's 200 ms
# staleness threshold makes endpoints read stale between scrapes; the detector's filter then drops them
# from the candidates and breaks session affinity. Left unset, the shipped value is used.
interval = os.environ.get("EPP_SCRAPE_INTERVAL", "")
if interval:
    e = sub(e, "  - name: metrics-source\n    type: metrics-data-source\n",
            f'  - name: metrics-source\n    type: metrics-data-source\n    parameters:\n      interval: "{interval}"\n')
if epp_variant == "guide":
    pass
elif epp_variant == "session":
    e = sub(e, "  - type: no-hit-lru-scorer\n",
            "  - type: no-hit-lru-scorer\n"
            "  - type: session-affinity-scorer\n"
            "    parameters:\n"
            "      strategy: session_id\n"
            "      sessionIdConfig:\n"
            "        sources:\n"
            "          - header: x-session-id\n")
    # Weight 10 against the other scorers' 2+2+3+2: a bound session wins unless its worker is excluded.
    e = sub(e, "      - pluginRef: no-hit-lru-scorer\n        weight: 2\n",
            "      - pluginRef: no-hit-lru-scorer\n        weight: 2\n"
            "      - pluginRef: session-affinity-scorer\n        weight: 10\n")
else:
    sys.exit("unknown epp variant " + epp_variant)
(out / "epp-config.yaml").write_text(e)
print("ok", out)
