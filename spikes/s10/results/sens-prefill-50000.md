## Sensitivity: block 64 tokens, prefill 50000 tokens/s (window 1200-4800 s; block 64 tokens; prefill 50000 tokens/s)

Means over seeds, with [min, max] for the hit rate. Columns marked sum or max are sums or maxima over seeds. Round robin is the first row of each table.

### KV cache 1500000 tokens per Instance

Round robin's time-averaged working set: 158 active conversations holding 6.04 M context tokens, 1.01x the cluster's KV cache (4 x 1500000 tokens).

| policy / key | hit rate | later turns | throughput vs RR (completed in window) | throughput vs RR (dispatched in window) | prefill tokens / request | mean / p95 latency (s) | moves | keys (top share) | peak in-flight (max) | max load / mean (max) | above bound, HBEP-5 rule (sum over seeds) | +1 over ceil(c*mean) (sum over seeds) | time above c*mean |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| round-robin | 36.5% [36.2%, 36.7%] | 35.9% | 1.00x | 1.00x | 23880 | 19.8 / 54.0 | 74.8% | - | 92 | 2.84 | - | - | - |
| least-load | 35.1% [34.7%, 35.8%] | 34.5% | 1.25x | 1.25x | 24319 | 15.0 / 33.1 | 74.4% | - | 36 | 1.33 | - | - | - |
| affinity / opening | 85.0% [77.6%, 91.2%] | 85.0% | 1.36x | 1.36x | 5654 | 13.8 / 39.0 | 0.4% | 2627 (0.0%) | 89 | 4.00 | - | - | - |
| chwbl-1.25 / opening | 89.6% [87.7%, 90.9%] | 89.7% | 1.61x | 1.61x | 3896 | 10.5 / 28.2 | 3.9% | 2977 (0.0%) | 41 | 1.60 | 0 | 326 (0.21% of placements) | 20.8% |
| chwbl-1.50 / opening | 87.5% [84.1%, 92.7%] | 87.6% | 1.55x | 1.55x | 4672 | 11.1 / 29.9 | 2.1% | 2879 (0.0%) | 50 | 2.00 | 0 | 237 (0.16% of placements) | 12.3% |
| chwbl-2.00 / opening | 84.8% [77.4%, 91.2%] | 84.8% | 1.41x | 1.41x | 5721 | 12.8 / 35.4 | 3.2% | 2690 (0.0%) | 69 | 2.50 | 0 | 1335 (0.97% of placements) | 13.9% |
| affinity / header>opening | 82.2% [76.3%, 91.6%] | 82.2% | 1.24x | 1.24x | 6700 | 15.8 / 44.7 | 0.2% | 2470 (0.0%) | 98 | 4.00 | - | - | - |
| chwbl-1.25 / header>opening | 89.8% [86.2%, 91.6%] | 89.8% | 1.60x | 1.60x | 3823 | 10.6 / 28.4 | 3.5% | 2970 (0.0%) | 41 | 1.60 | 0 | 305 (0.20% of placements) | 20.7% |
| chwbl-1.50 / header>opening | 85.9% [81.5%, 91.8%] | 85.9% | 1.51x | 1.51x | 5283 | 11.5 / 31.0 | 2.4% | 2850 (0.0%) | 52 | 2.00 | 0 | 278 (0.19% of placements) | 12.8% |
| chwbl-2.00 / header>opening | 82.0% [76.3%, 91.6%] | 81.9% | 1.33x | 1.33x | 6774 | 14.0 / 38.0 | 4.4% | 2593 (0.0%) | 70 | 2.50 | 0 | 1741 (1.31% of placements) | 18.6% |
