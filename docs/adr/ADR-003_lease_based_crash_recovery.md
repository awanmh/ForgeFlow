# ADR-003: Lease-Based Worker Ownership and Crash Recovery

## Status
Accepted

## Context
In a worker pool, a worker process may crash, lose network connectivity, or hang indefinitely due to unbounded external calls. If a worker permanently owns a job, that job becomes stuck indefinitely.

## Decision
ForgeFlow implements explicit lease-based job ownership with active heartbeats:
1. When a worker claims a job (`QUEUED` -> `RUNNING`), it sets a `lease_token` and `lease_expires_at = NOW() + LEASE_DURATION` (default 30s).
2. While processing, the worker periodically sends heartbeats (every 10s) to renew `lease_expires_at`.
3. If the worker crashes, heartbeats cease.
4. The scheduler periodically scans for `RUNNING` jobs where `lease_expires_at < NOW()`.
5. Expired jobs are marked with an `ABANDONED` attempt and transactionally transitioned to `RETRYING` (or `DEAD` if max attempts exceeded) for other workers to execute.

## Consequences
### Positive
- Automatic, autonomous recovery from hard crashes without manual intervention.
- Zero permanently orphan jobs.

### Negative
- Clock skew across nodes must be minimized via NTP.
- Tasks taking longer than expected must maintain regular heartbeats.
