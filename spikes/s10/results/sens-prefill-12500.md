## Sensitivity: block 64 tokens, prefill 12500 tokens/s (window 1200-4800 s; block 64 tokens; prefill 12500 tokens/s)

Means over seeds, with [min, max] for the hit rate. Columns marked sum or max are sums or maxima over seeds. Round robin is the first row of each table.

### KV cache 1500000 tokens per Instance

Round robin's time-averaged working set: 160 active conversations holding 6.27 M context tokens, 1.05x the cluster's KV cache (4 x 1500000 tokens).

| policy / key | hit rate | later turns | throughput vs RR (completed in window) | throughput vs RR (dispatched in window) | prefill tokens / request | mean / p95 latency (s) | moves | keys (top share) | peak in-flight (max) | max load / mean (max) | above bound, HBEP-5 rule (sum over seeds) | +1 over ceil(c*mean) (sum over seeds) | time above c*mean |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| round-robin | 45.3% [45.1%, 45.4%] | 44.8% | 1.00x | 1.00x | 21527 | 98.4 / 375.9 | 75.0% | - | 146 | 3.84 | - | - | - |
| least-load | 36.0% [35.8%, 36.1%] | 35.4% | 1.30x | 1.30x | 24784 | 74.4 / 111.8 | 69.8% | - | 40 | 1.33 | - | - | - |
| affinity / opening | 82.4% [71.7%, 88.1%] | 82.4% | 2.18x | 2.18x | 6577 | 43.9 / 194.7 | 0.4% | 1423 (0.1%) | 126 | 4.00 | - | - | - |
| chwbl-1.25 / opening | 74.5% [69.7%, 79.2%] | 74.4% | 2.88x | 2.87x | 9609 | 31.2 / 102.3 | 18.6% | 1595 (0.1%) | 50 | 1.60 | 0 | 789 (0.96% of placements) | 74.2% |
| chwbl-1.50 / opening | 74.0% [71.4%, 78.1%] | 73.8% | 2.54x | 2.54x | 9768 | 35.6 / 137.9 | 19.7% | 1488 (0.1%) | 58 | 2.00 | 0 | 1266 (1.66% of placements) | 73.5% |
| chwbl-2.00 / opening | 80.5% [67.3%, 87.2%] | 80.4% | 3.02x | 3.02x | 7207 | 36.1 / 152.6 | 7.3% | 1683 (0.1%) | 79 | 2.50 | 0 | 2255 (2.61% of placements) | 58.3% |
| affinity / header>opening | 86.5% [86.2%, 86.8%] | 86.6% | 2.20x | 2.19x | 4968 | 42.1 / 223.5 | 0.2% | 1428 (0.1%) | 140 | 4.00 | - | - | - |
| chwbl-1.25 / header>opening | 70.3% [61.2%, 76.1%] | 70.2% | 2.58x | 2.58x | 11185 | 36.5 / 110.0 | 21.6% | 1496 (0.1%) | 50 | 1.60 | 0 | 810 (1.06% of placements) | 76.6% |
| chwbl-1.50 / header>opening | 72.6% [70.7%, 74.4%] | 72.5% | 2.43x | 2.43x | 10322 | 37.1 / 138.5 | 21.1% | 1428 (0.1%) | 59 | 2.00 | 0 | 1389 (1.88% of placements) | 75.3% |
| chwbl-2.00 / header>opening | 86.6% [85.4%, 87.4%] | 86.6% | 3.72x | 3.72x | 5024 | 22.6 / 142.4 | 9.4% | 1901 (0.1%) | 76 | 2.50 | 0 | 2686 (2.76% of placements) | 66.8% |
