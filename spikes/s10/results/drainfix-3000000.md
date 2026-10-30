## Membership events (KV cache 3000000 tokens per Instance)

One row per event. Hit-rate windows start at that row's own event time: before = the 10 min before it, then 0-5 and 5-15 min after it, over requests dispatched in the window; the baseline is the same seed and policy with no event. Counts and hit rates are means over seeds; columns marked sum or max are sums or maxima over seeds. Raw per-seed values: drainfix-3000000-per-event.csv.

### drain

| policy / key | event | checked | re-homed: forced | collateral | to new | cut (whole run) | hit 10 min before | hit 0-5 min after (baseline) | hit 5-15 min after (baseline) | above bound, HBEP-5 rule (whole run, sum over seeds) | max over bound (req., max) | +1 over ceil (whole run, sum over seeds) | KubeAI total minus members' total (req., max) | KubeAI mean minus members' mean (req., max; the excess / members) |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| chwbl-1.25+release-on-drain / header>opening | drain instance-1 at 3000 s | 152 | 35 | 4 | 0 | 0 | 95.2% | 94.6% (95.1%) | 95.5% (95.3%) | 0 | 0.0 | 281 | 0 | 0.0 |
| chwbl-1.50+release-on-drain / header>opening | drain instance-1 at 3000 s | 153 | 34 | 1 | 0 | 0 | 96.1% | 95.1% (96.0%) | 96.0% (96.0%) | 0 | 0.0 | 132 | 0 | 0.0 |
| chwbl-2.00+release-on-drain / header>opening | drain instance-1 at 3000 s | 152 | 34 | 1 | 0 | 0 | 96.1% | 95.0% (96.0%) | 96.0% (96.0%) | 0 | 0.0 | 13 | 0 | 0.0 |
| chwbl-1.25+release-on-drain / opening | drain instance-1 at 3000 s | 151 | 40 | 4 | 0 | 0 | 95.5% | 94.3% (94.9%) | 95.5% (95.1%) | 0 | 0.0 | 218 | 0 | 0.0 |
| chwbl-1.50+release-on-drain / opening | drain instance-1 at 3000 s | 152 | 39 | 1 | 0 | 0 | 96.1% | 94.8% (96.0%) | 96.0% (95.7%) | 0 | 0.0 | 83 | 0 | 0.0 |
| chwbl-2.00+release-on-drain / opening | drain instance-1 at 3000 s | 152 | 39 | 1 | 0 | 0 | 96.1% | 94.8% (96.0%) | 96.0% (96.0%) | 0 | 0.0 | 10 | 0 | 0.0 |

### handover

| policy / key | event | checked | re-homed: forced | collateral | to new | cut (whole run) | hit 10 min before | hit 0-5 min after (baseline) | hit 5-15 min after (baseline) | above bound, HBEP-5 rule (whole run, sum over seeds) | max over bound (req., max) | +1 over ceil (whole run, sum over seeds) | KubeAI total minus members' total (req., max) | KubeAI mean minus members' mean (req., max; the excess / members) |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| chwbl-1.25+release-on-drain / header>opening | join instance-4 at 3000 s | 152 | 0 | 9 | 32 | 0 | 95.2% | 93.8% (95.1%) | 94.6% (95.3%) | 0 | 0.0 | 411 | 0 | 0.0 |
| chwbl-1.25+release-on-drain / header>opening | drain instance-1 at 3300 s | 147 | 28 | 2 | 0 | 0 | 94.5% | 94.1% (95.1%) | 95.1% (95.3%) | 0 | 0.0 | 411 | 0 | 0.0 |
| chwbl-1.50+release-on-drain / header>opening | join instance-4 at 3000 s | 153 | 0 | 1 | 33 | 0 | 96.1% | 95.1% (96.0%) | 95.6% (96.0%) | 0 | 0.0 | 52 | 0 | 0.0 |
| chwbl-1.50+release-on-drain / header>opening | drain instance-1 at 3300 s | 147 | 29 | 0 | 0 | 0 | 95.6% | 95.1% (95.9%) | 96.0% (96.0%) | 0 | 0.0 | 52 | 0 | 0.0 |
| chwbl-2.00+release-on-drain / header>opening | join instance-4 at 3000 s | 152 | 0 | 1 | 33 | 0 | 96.1% | 95.1% (96.0%) | 95.6% (96.0%) | 0 | 0.0 | 20 | 0 | 0.0 |
| chwbl-2.00+release-on-drain / header>opening | drain instance-1 at 3300 s | 147 | 29 | 0 | 0 | 0 | 95.7% | 95.1% (95.9%) | 96.0% (96.0%) | 0 | 0.0 | 20 | 0 | 0.0 |
| chwbl-1.25+release-on-drain / opening | join instance-4 at 3000 s | 151 | 0 | 8 | 30 | 0 | 95.5% | 93.8% (94.9%) | 94.5% (95.1%) | 0 | 0.0 | 430 | 0 | 0.0 |
| chwbl-1.25+release-on-drain / opening | drain instance-1 at 3300 s | 148 | 33 | 4 | 0 | 0 | 94.7% | 93.7% (94.8%) | 94.9% (95.0%) | 0 | 0.0 | 430 | 0 | 0.0 |
| chwbl-1.50+release-on-drain / opening | join instance-4 at 3000 s | 152 | 0 | 3 | 29 | 0 | 96.1% | 94.9% (96.0%) | 95.4% (95.7%) | 0 | 0.0 | 66 | 0 | 0.0 |
| chwbl-1.50+release-on-drain / opening | drain instance-1 at 3300 s | 148 | 35 | 1 | 0 | 0 | 95.5% | 94.9% (95.5%) | 95.9% (95.9%) | 0 | 0.0 | 66 | 0 | 0.0 |
| chwbl-2.00+release-on-drain / opening | join instance-4 at 3000 s | 152 | 0 | 1 | 30 | 0 | 96.1% | 95.2% (96.0%) | 95.6% (96.0%) | 0 | 0.0 | 24 | 0 | 0.0 |
| chwbl-2.00+release-on-drain / opening | drain instance-1 at 3300 s | 148 | 35 | 1 | 0 | 0 | 95.7% | 95.0% (95.9%) | 96.0% (96.0%) | 0 | 0.0 | 24 | 0 | 0.0 |
