# ADR-002: At-Least-Once Delivery and Idempotent Execution Semantics

## Status
Accepted

## Context
Distributed computing environments inherently experience network partitions, hardware faults, and unexpected worker crashes. When a worker completes an external side-effect (e.g. charging a payment, sending a webhook) but crashes before updating the database, the scheduler will detect lease expiration and requeue the job. Claiming "exactly-once" execution in such scenarios is physically impossible without two-phase commit across all external endpoints.

## Decision
ForgeFlow explicitly adopts and documents **at-least-once delivery with idempotent execution semantics**:
1. Delivery: Guaranteed at-least-once through durable queueing, lease management, and outbox reconciliation.
2. Execution: May be executed more than once upon worker crash recovery.
3. State transitions: Transactionally protected and guarded in PostgreSQL.
4. Business side-effects: Must be designed to be idempotent (e.g., using Idempotency Keys and idempotent task handlers).

## Consequences
### Positive
- Defensible, scientifically accurate distributed systems design.
- Transparent failure handling without false consistency claims.

### Negative
- Task authors must account for idempotency when performing external mutating operations.
