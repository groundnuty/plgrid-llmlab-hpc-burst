**Tight (1.5 M KV per Instance), full membership**

| option | hit rate [min, max] | throughput vs RR | p95 latency (s) | peak in flight (max over seeds) | time above 48 (s, sum over seeds) | hottest Instance: peak in flight / hit rate | placements above the bound (sum) | spills (sum) |
|---|---|---|---|---|---|---|---|---|
| round robin | 43.4% [42.9%, 44.0%] | 1.00x | 166.1 | 131 | 15305 | 129.3 / 17.0% | - | - |
| 1. c = 1.25 | 84.0% [79.1%, 90.7%] | 2.83x | 37.3 | 46 | 0 | 45.0 / 77.3% | 0 | 0 |
| 2. c = 2.0 | 82.1% [81.2%, 83.3%] | 2.13x | 78.5 | 75 | 12345 | 73.3 / 18.4% | 0 | 0 |
| 3. c = 2.0 + cap 48 | 75.2% [71.5%, 79.5%] | 2.10x | 61.2 | 48 | 0 | 48.0 / 36.7% | 0 | 9856 |
| 4. adaptive 1.25 / 2.0 | 84.0% [79.1%, 90.7%] | 2.83x | 37.3 | 46 | 0 | 45.0 / 77.3% | 0 | 0 |
| 5. adaptive 1.25 / 2.0, cap 48 at full membership | 84.0% [79.1%, 90.7%] | 2.83x | 37.3 | 46 | 0 | 45.0 / 77.3% | 0 | 0 |

**Scarce (750 k KV per Instance)**

| option | hit rate [min, max] | throughput vs RR | p95 latency (s) | peak in flight (max over seeds) | time above 48 (s, sum over seeds) | hottest Instance: peak in flight / hit rate | placements above the bound (sum) | spills (sum) |
|---|---|---|---|---|---|---|---|---|
| round robin | 27.9% [24.7%, 31.9%] | 1.00x | 125.4 | 122 | 15008 | 110.0 / 17.1% | - | - |
| 1. c = 1.25 | 30.1% [21.0%, 37.2%] | 1.07x | 71.0 | 49 | 6 | 48.7 / 16.7% | 0 | 0 |
| 2. c = 2.0 | 31.7% [22.7%, 37.9%] | 1.02x | 80.2 | 69 | 9734 | 64.3 / 17.0% | 0 | 0 |
| 3. c = 2.0 + cap 48 | 30.9% [21.7%, 38.0%] | 1.04x | 72.7 | 48 | 0 | 48.0 / 16.8% | 0 | 1865 |
| 4. adaptive 1.25 / 2.0 | 30.1% [21.0%, 37.2%] | 1.07x | 71.0 | 49 | 6 | 48.7 / 16.7% | 0 | 0 |
| 5. adaptive 1.25 / 2.0, cap 48 at full membership | 29.9% [21.0%, 37.3%] | 1.07x | 71.0 | 48 | 0 | 48.0 / 16.5% | 0 | 9 |

**Half prefill (12,500 tokens/s)**

| option | hit rate [min, max] | throughput vs RR | p95 latency (s) | peak in flight (max over seeds) | time above 48 (s, sum over seeds) | hottest Instance: peak in flight / hit rate | placements above the bound (sum) | spills (sum) |
|---|---|---|---|---|---|---|---|---|
| round robin | 45.3% [45.1%, 45.4%] | 1.00x | 375.9 | 146 | 18835 | 144.7 / 17.0% | - | - |
| 1. c = 1.25 | 70.3% [61.2%, 76.1%] | 2.58x | 110.0 | 50 | 228 | 49.3 / 48.4% | 0 | 0 |
| 2. c = 2.0 | 86.6% [85.4%, 87.4%] | 3.72x | 142.4 | 76 | 13247 | 74.0 / 18.4% | 0 | 0 |
| 3. c = 2.0 + cap 48 | 74.9% [73.8%, 75.5%] | 2.83x | 117.2 | 48 | 0 | 48.0 / 38.4% | 0 | 13265 |
| 4. adaptive 1.25 / 2.0 | 70.3% [61.2%, 76.1%] | 2.58x | 110.0 | 50 | 228 | 49.3 / 48.4% | 0 | 0 |
| 5. adaptive 1.25 / 2.0, cap 48 at full membership | 70.4% [61.6%, 76.1%] | 2.58x | 107.8 | 48 | 0 | 48.0 / 48.2% | 0 | 156 |

**After an unreplaced drain (instance-1 drained at 3,000 s)**

| option | hit 0-5 min after (no drain) | hit 5-15 min after (no drain) | hit 15-30 min after (no drain) | throughput vs RR from 3,000 s | p95 latency from 3,000 s (s) | peak in flight after the drain | time above 48 after the drain (s, sum over seeds) | held at the front (sum) |
|---|---|---|---|---|---|---|---|---|
| round robin | 41.5% (43.2%) | 42.3% (43.3%) | 41.7% (43.0%) | 1.00x | 183.8 | 140 | 5400 | - |
| 1. c = 1.25 | 52.9% (82.7%) | 51.6% (81.4%) | 50.5% (84.8%) | 1.62x | 79.3 | 65 | 5295 | 0 |
| 2. c = 2.0 | 77.1% (81.4%) | 76.6% (81.5%) | 76.0% (80.7%) | 2.12x | 110.7 | 103 | 5400 | 0 |
| 3. c = 2.0 + cap 48 | 22.3% (70.1%) | 17.5% (66.5%) | 21.1% (67.8%) | 1.06x | 83.1 | 48 | 0 | 11041 |
| 4. adaptive 1.25 / 2.0 | 62.5% (82.7%) | 57.8% (81.4%) | 57.8% (84.8%) | 1.64x | 92.8 | 96 | 5334 | 0 |
| 5. adaptive 1.25 / 2.0, cap 48 at full membership | 62.5% (82.7%) | 57.8% (81.4%) | 57.8% (84.8%) | 1.64x | 92.8 | 96 | 5334 | 0 |
