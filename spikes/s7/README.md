# Spike S7: checkpoint hashing on parallel filesystems

Status: in progress. This file is committed first with only the question and the time budget, so the budget is on record before any measurement.

## Question

HBEP-9 makes the checkpoint an input to the deployment ID and lists "file hashes against the revision" as the lean start-up verification. Two things are unknown:

1. How long does hashing a staged checkpoint take on a Helios compute node, reading from the cluster's Lustre scratch, and how many bytes does it actually read?
2. Do per-file SHA-256 hashes computed from the staged files equal the `lfs.sha256` that the Hugging Face Hub publishes for the same revision?

The decision: rehash every checkpoint file at every Instance start, or rehash once at staging and run a cheap check (size and mtime) at start.

## Time budget (set before any measurement)

Set on 2026-10-30, before the hashing job was submitted:

- **Verification at Instance start must add at most 5 minutes of wall time for any checkpoint up to 170 GB**, measured on a 4-CPU Helios CPU allocation reading from Lustre scratch.
- Reason: Instances run inside time-limited Allocations (HBEP-9, Operational readiness 4), and the GPU is held, and billed, while verification runs. Five minutes is about 2% of a 4-hour Allocation.
- Decision rule:
  - if a full rehash of the largest staged checkpoint fits the budget, recommend the full rehash at every start (no stored state to go stale, and it catches an edit that keeps size and mtime);
  - if it does not fit, recommend rehash at staging plus a cheap check at start, and state what the cheap check misses.
