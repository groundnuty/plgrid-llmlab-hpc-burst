## Membership events (KV cache 1500000 tokens per Instance)

One row per event. Hit-rate windows start at that row's own event time: before = the 10 min before it, then 0-5 and 5-15 min after it, over requests dispatched in the window; the baseline is the same seed and policy with no event. Counts and hit rates are means over seeds; columns marked sum or max are sums or maxima over seeds. Raw per-seed values: drainfix-1500000-per-event.csv.

### drain

| policy / key | event | checked | re-homed: forced | collateral | to new | cut (whole run) | hit 10 min before | hit 0-5 min after (baseline) | hit 5-15 min after (baseline) | above bound, HBEP-5 rule (whole run, sum over seeds) | max over bound (req., max) | +1 over ceil (whole run, sum over seeds) | KubeAI total minus members' total (req., max) | KubeAI mean minus members' mean (req., max; the excess / members) |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| chwbl-1.25+release-on-drain / header>opening | drain instance-1 at 3000 s | 149 | 32 | 5 | 0 | 0 | 81.2% | 52.9% (82.7%) | 51.6% (81.4%) | 0 | 0.0 | 1096 | 0 | 0.0 |
| chwbl-1.50+release-on-drain / header>opening | drain instance-1 at 3000 s | 153 | 27 | 13 | 0 | 0 | 73.2% | 58.5% (70.6%) | 53.2% (66.4%) | 0 | 0.0 | 1798 | 0 | 0.0 |
| chwbl-2.00+release-on-drain / header>opening | drain instance-1 at 3000 s | 152 | 32 | 24 | 0 | 0 | 80.5% | 77.1% (81.4%) | 76.6% (81.5%) | 0 | 0.0 | 2616 | 0 | 0.0 |
| chwbl-1.25+release-on-drain / opening | drain instance-1 at 3000 s | 152 | 42 | 11 | 0 | 0 | 71.8% | 57.1% (85.6%) | 54.8% (91.2%) | 0 | 0.0 | 1056 | 0 | 0.0 |
| chwbl-1.50+release-on-drain / opening | drain instance-1 at 3000 s | 152 | 42 | 18 | 0 | 0 | 77.3% | 64.7% (76.3%) | 59.5% (76.8%) | 0 | 0.0 | 2160 | 0 | 0.0 |
| chwbl-2.00+release-on-drain / opening | drain instance-1 at 3000 s | 154 | 29 | 23 | 0 | 0 | 81.6% | 78.0% (81.1%) | 76.6% (81.8%) | 0 | 0.0 | 2386 | 0 | 0.0 |

### handover

| policy / key | event | checked | re-homed: forced | collateral | to new | cut (whole run) | hit 10 min before | hit 0-5 min after (baseline) | hit 5-15 min after (baseline) | above bound, HBEP-5 rule (whole run, sum over seeds) | max over bound (req., max) | +1 over ceil (whole run, sum over seeds) | KubeAI total minus members' total (req., max) | KubeAI mean minus members' mean (req., max; the excess / members) |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| chwbl-1.25+release-on-drain / header>opening | join instance-4 at 3000 s | 149 | 0 | 14 | 34 | 0 | 81.2% | 90.1% (82.7%) | 89.5% (81.4%) | 0 | 0.0 | 712 | 0 | 0.0 |
| chwbl-1.25+release-on-drain / header>opening | drain instance-1 at 3300 s | 148 | 25 | 10 | 0 | 0 | 87.5% | 88.0% (78.3%) | 89.6% (84.5%) | 0 | 0.0 | 712 | 0 | 0.0 |
| chwbl-1.50+release-on-drain / header>opening | join instance-4 at 3000 s | 153 | 0 | 22 | 36 | 0 | 73.2% | 85.4% (70.6%) | 79.6% (66.4%) | 0 | 0.0 | 1018 | 0 | 0.0 |
| chwbl-1.50+release-on-drain / header>opening | drain instance-1 at 3300 s | 148 | 22 | 12 | 0 | 0 | 80.5% | 79.3% (66.2%) | 76.7% (68.1%) | 0 | 0.0 | 1018 | 0 | 0.0 |
| chwbl-2.00+release-on-drain / header>opening | join instance-4 at 3000 s | 152 | 0 | 20 | 40 | 0 | 80.5% | 84.3% (81.4%) | 82.5% (81.5%) | 0 | 0.0 | 3336 | 0 | 0.0 |
| chwbl-2.00+release-on-drain / header>opening | drain instance-1 at 3300 s | 150 | 22 | 22 | 0 | 0 | 83.1% | 81.9% (81.3%) | 82.7% (81.7%) | 0 | 0.0 | 3336 | 0 | 0.0 |
| chwbl-1.25+release-on-drain / opening | join instance-4 at 3000 s | 152 | 0 | 20 | 35 | 0 | 71.8% | 91.0% (85.6%) | 89.1% (91.2%) | 0 | 0.0 | 591 | 0 | 0.0 |
| chwbl-1.25+release-on-drain / opening | drain instance-1 at 3300 s | 152 | 31 | 8 | 0 | 0 | 84.8% | 87.8% (90.5%) | 91.6% (91.4%) | 0 | 0.0 | 591 | 0 | 0.0 |
| chwbl-1.50+release-on-drain / opening | join instance-4 at 3000 s | 152 | 0 | 19 | 39 | 0 | 77.3% | 88.4% (76.3%) | 75.3% (76.8%) | 0 | 0.0 | 945 | 0 | 0.0 |
| chwbl-1.50+release-on-drain / opening | drain instance-1 at 3300 s | 150 | 34 | 11 | 0 | 0 | 83.7% | 77.4% (76.6%) | 72.6% (74.9%) | 0 | 0.0 | 945 | 0 | 0.0 |
| chwbl-2.00+release-on-drain / opening | join instance-4 at 3000 s | 154 | 0 | 26 | 42 | 0 | 81.6% | 84.0% (81.1%) | 82.4% (81.8%) | 0 | 0.0 | 2986 | 0 | 0.0 |
| chwbl-2.00+release-on-drain / opening | drain instance-1 at 3300 s | 148 | 28 | 24 | 0 | 0 | 83.1% | 82.5% (82.2%) | 82.2% (81.6%) | 0 | 0.0 | 2986 | 0 | 0.0 |
