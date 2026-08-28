# ForgeFlow Production Readiness Audit & Hardening Plan

This document establishes the technical audit, failure injection test specifications, security reviews, full-stack Docker validation, and **minimalist, premium (Apple / Vercel / Linear-grade)** UI redesign.

---

## 1. Technical Audit & Distributed Semantics

### Queue Lifecycle & Delivery Semantics
```text
Client (POST /api/v1/jobs)
   │
   ▼
PostgreSQL Transaction (Authoritative Truth)
   ├── INSERT INTO jobs (status = 'PENDING')
   └── INSERT INTO outbox_events (status = 'PENDING')
   │
   ├─► Immediate Redis Stream (XADD) [Best-effort Fast Path]
   └─► Async Outbox Publisher [Guaranteed Reconciliation Path]
          │
          ▼
     Redis Stream (forgeflow:stream:queue_name)
          │
          ▼
     Consumer Group (XREADGROUP)
          │
          ▼
     Worker Atomic Claim (PostgreSQL: UPDATE jobs SET status = 'RUNNING' FOR UPDATE SKIP LOCKED)
          │
          ├── Heartbeat Goroutine (Periodically renews lease in PostgreSQL)
          ├── Task Execution (Bounded Worker Semaphore)
          │
          ▼
     PostgreSQL Terminal Transition (status = 'SUCCEEDED' | 'RETRYING' | 'DEAD')
          │
          ▼
     Redis ACK (XACK + XDEL)
```

#### Failure Modes & Formal Guarantees
1. **Worker Crash After Side Effect But Before ACK**:
   - **Scenario**: Worker finishes sending a payment or external request, but its process crashes before updating PostgreSQL or sending `XACK`.
   - **Resolution**: Lease expires (`lease_expires_at < NOW()`). Scheduler's Lease Recovery daemon resets the job to `RETRYING` or reclaims it for a new worker attempt.
   - **Invariant**: ForgeFlow guarantees **At-Least-Once Delivery**. Handlers MUST be idempotent.
2. **Zombie Worker & Fencing Safety**:
   - **Scenario**: Worker A experiences a severe GC pause or network partition. Its lease expires, and Scheduler assigns Attempt #2 to Worker B. Worker A recovers and attempts to mark the job `SUCCEEDED`.
   - **Resolution**: PostgreSQL update query requires `WHERE id = $1 AND attempt_number = $2 AND worker_id = $3`. Because Attempt #2 is active with Worker B's token, Worker A's update affects 0 rows and is rejected.
3. **Redis Outage / Data Loss**:
   - **Scenario**: Redis crashes or restarts without persistence.
   - **Resolution**: PostgreSQL remains the source of truth. The Transactional Outbox Publisher discovers pending events and replenishes the Redis Stream automatically.

---

## 2. Concurrency & Failure Injection Test Matrix

| Test Case | Scenario | Expected Invariant |
| :--- | :--- | :--- |
| **Idempotency Concurrency Race** | 50 concurrent goroutines submit the same `Idempotency-Key` at the exact same instant | Exactly 1 logical job created in PostgreSQL; 49 requests receive identical idempotent replay |
| **Workflow Diamond Concurrency Race** | `Build -> (Test, Security) -> Deploy`. Both `Test` and `Security` finish simultaneously | `Deploy` child job is materialized and enqueued **exactly once** via atomic status transitions |
| **Zombie Worker Fencing** | Worker A stalls beyond lease duration; Worker B claims Attempt #2; Worker A resumes | Worker A's write is rejected; Worker B's outcome is authoritative |
| **Redis Outage Resilience** | Jobs submitted while Redis is offline | Jobs saved in PostgreSQL Outbox; automatically published to Redis once connection recovers |

---

## 3. Frontend Redesign: Minimalist Infrastructure Console (Apple / Vercel / Linear Aesthetic)

### Visual Principles
- **Neutral / Monochrome Palette**: Deep Obsidian `#09090b` (Zinc-950), Dark Carbon `#18181b` (Zinc-900), Slate `#27272a` (Zinc-800), Pure White `#fafafa`.
- **Restrained Accents**: Subtle status indicators (Emerald dot for healthy/succeeded, Amber for running/leased, Rose for failed, Zinc for pending).
- **Typography**: Inter + JetBrains Mono for monospace hashes, timestamps, and parameters.
- **Zero AI Clichés**: No heavy neon glowing drop-shadows, no cyberpunk gradients, no decorative clutter.

---

## 4. Full-Stack Docker Compose Verification

Ensure `docker compose up --build` boots the entire platform without manual host commands:
- `postgres`: Migrations auto-applied
- `redis`: In-memory Streams + AOF
- `api`: REST HTTP engine + Embedded UI (`/ui`)
- `worker`: Bounded concurrency execution engine
- `scheduler`: Leader election, lease recovery & outbox reconciler
- `prometheus` & `grafana`: Pre-configured dashboards & metrics scraping
