# ADR-001: PostgreSQL as Authoritative Source of Truth

## Status
Accepted

## Context
In distributed job scheduling systems, coordination state and execution history can easily diverge if multiple datastores claim authority over business state. Redis is frequently used for queueing and caching due to low latency, but lacks rich transactional guarantees across complex multi-table invariants and durable point-in-time recovery.

## Decision
PostgreSQL is established as the single authoritative System of Record (SoR) for all ForgeFlow business entities:
- Jobs and execution state transitions
- Historical job attempts
- Workflow DAG definitions and node dependencies
- Worker registration and heartbeat tracking
- Idempotency records
- Outbox events and audit logs

Redis is restricted to an ephemeral coordination layer:
- Job queueing / streams
- Distributed locks with ownership tokens
- Pub/Sub for real-time SSE propagation
- Rate limiting

If Redis fails or restarts, system state is never permanently lost and can be reconciled deterministically from PostgreSQL.

## Consequences
### Positive
- Strict ACID transactions and foreign key invariants protect execution history.
- Zero state loss window if Redis fails.
- Reliable historical auditing and attempt debugging.

### Negative
- PostgreSQL write throughput must be monitored with connection pooling (`pgxpool`) and efficient indexes.
