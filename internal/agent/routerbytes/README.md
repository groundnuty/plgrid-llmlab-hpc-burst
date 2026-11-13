# routerbytes

Spike S13's request cases, raw JSON comparison and recording worker (`spikes/s13`, PR #324), copied into
the root module for #429's Bytes test, which sends every S13 variant through the agent's serving path and
the agent-launched Envoy to a recording worker (team-lead's answer T4).

- `cases/`, `rawjson/`, `recorder/`: S13's Go files, byte for byte below a provenance block that ends
  with the line `// End of provenance.`, with S13's import paths mapped to this directory.
- `fields/vllm-v0.32.0.tsv`: S13's field enumeration, byte for byte, which `cases`' tests read.

`drift_test.go` checks each copy against `spikes/s13`: a change to either side fails it, and a deliberate
change is made to both.
