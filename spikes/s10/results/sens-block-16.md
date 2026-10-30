## Sensitivity: block 16 tokens, prefill 25000 tokens/s (window 1200-4800 s; block 16 tokens; prefill 25000 tokens/s)

Means over seeds, with [min, max] for the hit rate. Columns marked sum or max are sums or maxima over seeds. Round robin is the first row of each table.

### KV cache 1500000 tokens per Instance

Round robin's time-averaged working set: 159 active conversations holding 6.20 M context tokens, 1.03x the cluster's KV cache (4 x 1500000 tokens).

| policy / key | hit rate | later turns | throughput vs RR (completed in window) | throughput vs RR (dispatched in window) | prefill tokens / request | mean / p95 latency (s) | moves | keys (top share) | peak in-flight (max) | max load / mean (max) | above bound, HBEP-5 rule (sum over seeds) | +1 over ceil(c*mean) (sum over seeds) | time above c*mean |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| round-robin | 41.1% [39.0%, 43.5%] | 40.6% | 1.00x | 1.00x | 22510 | 46.0 / 160.5 | 75.0% | - | 132 | 3.61 | - | - | - |
| least-load | 35.3% [34.0%, 36.3%] | 34.7% | 1.29x | 1.29x | 24614 | 34.5 / 55.9 | 72.8% | - | 40 | 1.33 | - | - | - |
| affinity / opening | 83.5% [82.0%, 84.4%] | 83.4% | 1.61x | 1.61x | 6223 | 26.7 / 111.7 | 0.4% | 1786 (0.1%) | 124 | 4.00 | - | - | - |
| chwbl-1.25 / opening | 86.2% [83.5%, 91.3%] | 86.2% | 2.86x | 2.85x | 5176 | 12.9 / 38.5 | 7.1% | 2647 (0.0%) | 48 | 1.60 | 0 | 532 (0.39% of placements) | 35.9% |
| chwbl-1.50 / opening | 74.4% [67.6%, 81.4%] | 74.3% | 1.99x | 1.99x | 9616 | 21.1 / 64.5 | 13.9% | 2024 (0.0%) | 57 | 2.00 | 0 | 1240 (1.19% of placements) | 55.7% |
| chwbl-2.00 / opening | 82.8% [81.7%, 83.3%] | 82.7% | 2.14x | 2.14x | 6465 | 18.7 / 76.5 | 10.6% | 2125 (0.0%) | 74 | 2.50 | 0 | 3362 (3.07% of placements) | 51.4% |
| affinity / header>opening | 82.5% [80.6%, 83.4%] | 82.4% | 1.51x | 1.51x | 6542 | 29.3 / 119.4 | 0.2% | 1749 (0.1%) | 132 | 4.00 | - | - | - |
| chwbl-1.25 / header>opening | 84.7% [81.4%, 90.0%] | 84.7% | 2.82x | 2.82x | 5746 | 13.1 / 37.7 | 9.3% | 2640 (0.0%) | 46 | 1.60 | 0 | 640 (0.47% of placements) | 40.6% |
| chwbl-1.50 / header>opening | 75.1% [70.2%, 84.8%] | 75.0% | 2.04x | 2.04x | 9327 | 20.8 / 62.2 | 11.9% | 2093 (0.0%) | 57 | 2.00 | 0 | 1184 (1.11% of placements) | 51.7% |
| chwbl-2.00 / header>opening | 81.3% [77.9%, 83.1%] | 81.2% | 2.06x | 2.06x | 7019 | 19.8 / 79.9 | 11.1% | 2109 (0.0%) | 76 | 2.50 | 0 | 3440 (3.17% of placements) | 53.5% |
