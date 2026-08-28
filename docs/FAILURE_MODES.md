# ForgeFlow Failure Modes, Guarantees & Recovery Matrix

This document formally specifies the distributed systems behavior, failure handling invariants, and recovery semantics of the **ForgeFlow** platform.

---

## 1. System Guarantees & Contract

ForgeFlow makes strict, defensible distributed systems guarantees:

```text
┌───────────────────────────────────────┬─────────────────────────────────────────────────────────────┐
│ Dimension                             │ Formal Guarantee                                            │
├───────────────────────────────────────┼─────────────────────────────────────────────────────────────┤
│ Delivery Semantics                    │ AT-LEAST-ONCE Delivery (PostgreSQL Outbox + Redis Streams)  │
│ System of Record                      │ PostgreSQL (Authoritative Durable State)                   │
│ Concurrent Worker Ownership           │ Single Active Leaseholder per Job (Fenced by Lease Expiry)  │
│ Task Execution                        │ At-Least-Once (Handlers MUST be idempotent)                 │
│ API Job Creation                      │ Exactly-One Logical Job per Idempotency-Key                 │
│ Workflow DAG Dependency Resolution    │ Topologically ordered, cycle-free (Kahn's Algorithm)        │
└───────────────────────────────────────┴─────────────────────────────────────────────────────────────┘
```

> [!IMPORTANT]
> **Why Not Exactly-Once Execution?**
> In distributed systems across network partitions and unannounced process termination, magic "exactly-once execution" is impossible. If a worker completes an external side effect (e.g., charge credit card, send SMS) and immediately experiences a crash before ACKing or committing state to PostgreSQL, the scheduler will safely recover the job and retry. Therefore, **all task handlers must be designed to be idempotent** (using transaction IDs or idempotency tokens).

---

## 2. Comprehensive Failure Matrix

| Failure Scenario | Immediate Impact | System Reaction & Recovery Path | Data Loss Risk |
| :--- | :--- | :--- | :---: |
| **API Process Crash** | In-flight HTTP request drops. | Any committed transaction in PostgreSQL remains durable. Client receives connection error and retries with the same `Idempotency-Key`. | **Zero** |
| **Redis Outage / Crash** | Fast-path queue ingestion fails. | API commits Job (`PENDING`) and Outbox Event (`PENDING`) directly to PostgreSQL. Once Redis recovers, Scheduler Outbox Publisher drains the queue and replenishes Redis Streams. | **Zero** |
| **PostgreSQL Outage** | Write transactions fail. | API returns HTTP 500/503. System fails fast and never fabricates false success. | **Zero** |
| **Worker Process Crash / SIGKILL** | Job in `RUNNING` state remains uncompleted; heartbeat stops. | PostgreSQL lease expires (`lease_expires_at < NOW()`). Scheduler's Lease Recovery daemon discovers the expired lease, increments attempt count, and transitions job to `RETRYING` for another worker. | **Zero** |
| **Worker Network Partition / Long GC Pause (Zombie Worker)** | Worker A stalls; lease expires; Worker B reclaims and executes Job (Attempt #2). Worker A resumes and attempts to mark job `SUCCEEDED`. | PostgreSQL update query enforces fencing: `WHERE id = $1 AND worker_id = $2 AND lease_expires_at > NOW()`. Worker A's stale write affects **0 rows** (`ErrStaleWorkerWrite`) and is discarded. Worker B's result is authoritative. | **Zero** |
| **Task Panic / Unhandled Exception** | Worker goroutine encounters runtime panic. | Panic recovery middleware intercepts the panic, logs stack trace, records `FAILED` attempt with error message, and applies exponential backoff with full jitter. | **Zero** |
| **Task Execution Timeout** | Task runs longer than configured `timeout_seconds`. | Go `context.WithTimeout` fires `context.DeadlineExceeded`, cancelling task execution and marking attempt failed. | **Zero** |
| **Duplicate API Submission** | Client sends 50+ concurrent requests with identical `Idempotency-Key`. | PostgreSQL atomic `UNIQUE(user_id, idempotency_key)` constraint ensures exactly 1 logical job is created; 49 concurrent requests await completion and receive idempotent replayed response. | **Zero** |
| **Scheduler Process Crash** | Active leader scheduler dies. | Standby scheduler instances detect missing leader heartbeat/lease lock in Redis, acquire distributed leadership, and resume periodic sweeps seamlessly. | **Zero** |
| **Workflow Diamond Race Condition** | Two parent tasks (`Test`, `Security`) finish simultaneously and evaluate child task (`Deploy`). | Workflow-level mutex and PostgreSQL atomic status check (`WHERE id = $1 AND status = 'PENDING'`) ensure `Deploy` child job is materialized **exactly once**. | **Zero** |

---

## 3. Worker Lease & Fencing Protocol

```text
Worker A                              PostgreSQL                              Worker B
   │                                      │                                      │
   ├─ Claim Job (Lease: 30s) ────────────►│ (status='RUNNING', worker=A)         │
   │                                      │                                      │
   │ [GC Pause / Network Freeze]          │                                      │
   │ (Heartbeat stops)                    │                                      │
   │                                      ├─ [Lease Expires at t=30s]            │
   │                                      │                                      │
   │                                      │◄── Claim Job (Lease Recovery) ───────┤
   │                                      │    (status='RUNNING', worker=B)      │
   │                                      │                                      │
   │ [Wakes Up from GC Pause]             │                                      ├─ Executes Task
   ├─ UPDATE jobs (worker=A, lease>now) ─►│                                      │
   │  ◄── 0 Rows Affected (REJECTED) ─────┤                                      │
   │                                      │                                      │
   │                                      │◄── UPDATE jobs (worker=B) ───────────┤
   │                                      │    (status='SUCCEEDED') ────────────►│ (SUCCESS)
```

---

## 4. Architectural Trade-offs

### Frontend: Minimalist Embedded UI vs SSR Framework
- **Decision**: Serve a lightweight, high-performance vanilla HTML5/CSS/JavaScript console (`web/`) directly from the Go API binary under `/ui`.
- **Rationale**: Keeps ForgeFlow self-contained in a single deployable artifact without requiring a Node.js server, Next.js build runtime, or heavy frontend proxy layer for dashboard operations.

### CI/CD: GitHub Actions (Primary) + Jenkins (Enterprise Demonstration)
- **Decision**: GitHub Actions runs PR validation, linting, race-detector tests, and container builds on every commit. Jenkins serves as our on-premise secondary CI pipeline demonstration.
