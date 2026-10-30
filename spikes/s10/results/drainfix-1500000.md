## Membership events (KV cache 1500000 tokens per Instance; event at 3000 s; mean over seeds)

### drain

| policy / key | event | checked | re-homed: forced | collateral | to new | cut | hit before | hit 0-5 min after (baseline) | hit 5-15 min after (baseline) | above bound (HBEP-5 rule) | max over bound (req.) | +1 over ceil | KubeAI mean inflation (max req.) |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| chwbl-1.25+release-on-drain / header>opening | drain instance-1 at 3000 s | 149 | 32 | 5 | 0 | 0 | 81.2% | 52.9% (82.7%) | 51.6% (81.4%) | 0 | 0.0 | 1096 | 0 |
| chwbl-1.50+release-on-drain / header>opening | drain instance-1 at 3000 s | 153 | 27 | 13 | 0 | 0 | 73.2% | 58.5% (70.6%) | 53.2% (66.4%) | 0 | 0.0 | 1798 | 0 |
| chwbl-2.00+release-on-drain / header>opening | drain instance-1 at 3000 s | 152 | 32 | 24 | 0 | 0 | 80.5% | 77.1% (81.4%) | 76.6% (81.5%) | 0 | 0.0 | 2616 | 0 |
| chwbl-1.25+release-on-drain / opening | drain instance-1 at 3000 s | 152 | 42 | 11 | 0 | 0 | 71.8% | 57.1% (85.6%) | 54.8% (91.2%) | 0 | 0.0 | 1056 | 0 |
| chwbl-1.50+release-on-drain / opening | drain instance-1 at 3000 s | 152 | 42 | 18 | 0 | 0 | 77.3% | 64.7% (76.3%) | 59.5% (76.8%) | 0 | 0.0 | 2160 | 0 |
| chwbl-2.00+release-on-drain / opening | drain instance-1 at 3000 s | 154 | 29 | 23 | 0 | 0 | 81.6% | 78.0% (81.1%) | 76.6% (81.8%) | 0 | 0.0 | 2386 | 0 |
