# FORGEFLOW

> **Self-hosted distributed job processing and workflow orchestration platform with PostgreSQL-backed state, Redis coordination, and horizontally scalable Go workers.**

[![CI](https://github.com/forgeflow/forgeflow/actions/workflows/ci.yml/badge.svg)](https://github.com/forgeflow/forgeflow/actions)
[![Go Report Card](https://goreportcard.com/badge/github.com/forgeflow/forgeflow)](https://goreportcard.com/report/github.com/forgeflow/forgeflow)
[![License: MIT](https://img.shields.io/badge/License-MIT-black.svg)](LICENSE)

---

## 1. Overview

ForgeFlow is a production-grade distributed execution platform built for asynchronous background processing, scheduled jobs, crash recovery, and multi-step directed acyclic graph (DAG) workflow orchestration.

### Core Architectural Guarantees
* **PostgreSQL System of Record**: Authoritative state, attempt history, and DAG definitions persist in PostgreSQL.
* **At-Least-Once Delivery**: No committed job is silently lost upon Redis or worker restart.
* **Idempotent Submission**: Concurrent requests sharing an `Idempotency-Key` produce exactly one logical job.
* **Lease-Based Crash Recovery**: Dead worker jobs are automatically identified and recovered upon lease expiration.
* **Bounded Concurrency**: Worker pool execution is bound by semaphore limits to prevent uncontrolled goroutine growth.
* **Observable by Design**: Native Prometheus metrics, structured contextual JSON logs, and real-time SSE stream.

---

## 2. System Architecture

```text
                               INTERNET
                                   │
                                   ▼
                         ┌─────────────────┐
                         │      NGINX      │
                         │ Reverse Proxy   │
                         └────────┬────────┘
                                  │
                  ┌───────────────┴───────────────┐
                  │                               │
                  ▼                               ▼
          ┌───────────────┐                ┌──────────────┐
          │   Next.js     │                │ ForgeFlow API│
          │   Dashboard   │                │    Go / Gin  │
          └───────────────┘                └───────┬──────┘
                                                   │
                              ┌────────────────────┼─────────────────┐
                              │                    │                 │
                              ▼                    ▼                 ▼
                       ┌─────────────┐      ┌─────────────┐   ┌─────────────┐
                       │ PostgreSQL  │      │    Redis    │   │     SSE     │
                       │ STATE TRUTH │      │ Queue/Locks │   │     Hub     │
                       │ Jobs/DAG    │      │ PubSub/Rate │   └─────────────┘
                       └──────┬──────┘      └──────┬──────┘
                              │                    │
                    ┌─────────┴────────────────────┴──────────┐
                    │                                         │
                    ▼                                         ▼
             ┌──────────────┐                          ┌──────────────┐
             │  Scheduler   │                          │ Worker Pool  │
             │ Lease/Retry  │                          │ Bounded Pool │
             │ DAG Resolve  │                          │ Task Registry│
             └──────────────┘                          └──────────────┘
```

---

## 3. Technology Stack

| Layer | Technology | Purpose |
| :--- | :--- | :--- |
| **Core Backend** | Go 1.24+ | API, Workers, Scheduler |
| **HTTP Framework** | Gin | High-performance REST Routing |
| **Authoritative DB** | PostgreSQL 16+ | State of record, Attempts, DAG, Idempotency |
| **DB Driver** | pgx/v5 (`pgxpool`) | Connection pooling, row locking |
| **Coordination** | Redis 7+ (`go-redis/v9`) | Streams, Distributed Locks, Pub/Sub |
| **Observability** | Prometheus + Grafana | Metrics collection & dashboard visualization |
| **Structured Logs** | Go `log/slog` | Structured JSON logs with trace correlation |
| **Containerization**| Docker + Compose | Multi-stage unprivileged builds |

---

## 4. Repository Structure

```text
ForgeFlow/
├── cmd/
│   ├── api/             # API server entrypoint
│   ├── worker/          # Execution worker pool entrypoint
│   └── scheduler/       # Cron, lease recovery & DAG engine entrypoint
├── internal/
│   ├── domain/          # Core entities, state machines & domain errors
│   ├── application/     # Use cases & business workflow orchestration
│   ├── ports/           # Ports & abstractions (Repositories, Queue, Locker)
│   ├── infrastructure/  # Adapters (PostgreSQL, Redis, Logging, Config)
│   └── interfaces/      # HTTP handlers, middleware, SSE
├── migrations/          # Versioned SQL migrations (golang-migrate)
├── deployments/         # Nginx, Prometheus & Grafana configurations
├── tests/               # Integration, concurrency, and load testing
├── docker-compose.yml   # Multi-service local environment
├── Dockerfile           # Multi-stage container build
└── Makefile             # Developer automation commands
```

---

## 5. Getting Started

### Prerequisites
* Go 1.24+
* Docker & Docker Compose
* Make

### Local Quickstart

```bash
# Clone the repository
git clone https://github.com/forgeflow/forgeflow.git
cd forgeflow

# Copy environment configuration
cp .env.example .env

# Run unit tests with race detection
make test-race

# Build local binaries
make build

# Start full infrastructure stack via Docker Compose
make docker-up
```

### Health Check

```bash
curl -i http://localhost:8080/api/v1/health
curl -i http://localhost:8080/api/v1/ready
```

---

## 6. Architecture Decision Records (ADRs)

* [ADR-001: PostgreSQL as Authoritative Source of Truth](docs/adr/ADR-001_postgresql_source_of_truth.md)
* [ADR-002: At-Least-Once Delivery and Idempotent Execution](docs/adr/ADR-002_at_least_once_delivery.md)
* [ADR-003: Lease-Based Worker Ownership and Crash Recovery](docs/adr/ADR-003_lease_based_crash_recovery.md)

---

## 7. License

Distributed under the MIT License.
