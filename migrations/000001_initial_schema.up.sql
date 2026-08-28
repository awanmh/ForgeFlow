-- ForgeFlow Initial Schema Migration (Up)
CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- 1. Users table
CREATE TABLE IF NOT EXISTS users (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    email VARCHAR(320) NOT NULL,
    password_hash TEXT NOT NULL,
    name VARCHAR(120) NOT NULL,
    status VARCHAR(20) NOT NULL DEFAULT 'ACTIVE',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_login_at TIMESTAMPTZ,

    CONSTRAINT users_email_unique UNIQUE (email),
    CONSTRAINT users_status_check CHECK (status IN ('ACTIVE', 'DISABLED', 'LOCKED'))
);

CREATE INDEX IF NOT EXISTS idx_users_status ON users(status);

-- 2. Roles table
CREATE TABLE IF NOT EXISTS roles (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name VARCHAR(50) NOT NULL UNIQUE,
    description TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Seed baseline roles
INSERT INTO roles (name, description) VALUES
    ('ADMIN', 'Full system administrator'),
    ('OPERATOR', 'Operational operator with execution and queue controls'),
    ('USER', 'Standard user with job and workflow submission permissions'),
    ('VIEWER', 'Read-only observer')
ON CONFLICT (name) DO NOTHING;

-- 3. User Roles join table
CREATE TABLE IF NOT EXISTS user_roles (
    user_id UUID NOT NULL,
    role_id UUID NOT NULL,
    assigned_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    PRIMARY KEY (user_id, role_id),
    CONSTRAINT fk_user_roles_user FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE,
    CONSTRAINT fk_user_roles_role FOREIGN KEY (role_id) REFERENCES roles(id) ON DELETE CASCADE
);

-- 4. Queues table
CREATE TABLE IF NOT EXISTS queues (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name VARCHAR(100) NOT NULL UNIQUE,
    description TEXT,
    max_concurrency INTEGER,
    priority_levels INTEGER NOT NULL DEFAULT 10,
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT queues_priority_levels_check CHECK (priority_levels BETWEEN 1 AND 100),
    CONSTRAINT queues_concurrency_check CHECK (max_concurrency IS NULL OR max_concurrency > 0)
);

-- Seed default queue
INSERT INTO queues (name, description, priority_levels, enabled) VALUES
    ('default', 'Default general-purpose job queue', 10, TRUE),
    ('high-priority', 'Priority job queue for latency-sensitive tasks', 10, TRUE),
    ('background', 'Background low-priority processing queue', 10, TRUE)
ON CONFLICT (name) DO NOTHING;

-- 5. Workers table
CREATE TABLE IF NOT EXISTS workers (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    worker_key VARCHAR(128) NOT NULL UNIQUE,
    hostname VARCHAR(255),
    version VARCHAR(50),
    status VARCHAR(20) NOT NULL DEFAULT 'STARTING',
    concurrency INTEGER NOT NULL,
    registered_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_heartbeat_at TIMESTAMPTZ,
    started_at TIMESTAMPTZ,
    stopped_at TIMESTAMPTZ,
    capabilities JSONB NOT NULL DEFAULT '[]'::jsonb,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,

    CONSTRAINT workers_status_check CHECK (status IN ('STARTING', 'ACTIVE', 'DRAINING', 'STOPPED', 'DEAD')),
    CONSTRAINT workers_concurrency_check CHECK (concurrency > 0)
);

CREATE INDEX IF NOT EXISTS idx_workers_heartbeat ON workers(last_heartbeat_at);
CREATE INDEX IF NOT EXISTS idx_workers_status ON workers(status);

-- 6. Workflows table
CREATE TABLE IF NOT EXISTS workflows (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id UUID NOT NULL,
    name VARCHAR(200) NOT NULL,
    status VARCHAR(30) NOT NULL DEFAULT 'PENDING',
    total_nodes INTEGER NOT NULL DEFAULT 0,
    completed_nodes INTEGER NOT NULL DEFAULT 0,
    failed_nodes INTEGER NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    cancelled_at TIMESTAMPTZ,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,

    CONSTRAINT workflows_status_check CHECK (status IN ('PENDING', 'RUNNING', 'SUCCEEDED', 'FAILED', 'CANCELLED')),
    CONSTRAINT fk_workflows_user FOREIGN KEY (user_id) REFERENCES users(id)
);

CREATE INDEX IF NOT EXISTS idx_workflows_user ON workflows(user_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_workflows_status ON workflows(status);

-- 7. Workflow Nodes table
CREATE TABLE IF NOT EXISTS workflow_nodes (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    workflow_id UUID NOT NULL,
    node_key VARCHAR(100) NOT NULL,
    task_type VARCHAR(100) NOT NULL,
    payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    priority INTEGER NOT NULL DEFAULT 0,
    status VARCHAR(30) NOT NULL DEFAULT 'PENDING',
    max_attempts INTEGER NOT NULL DEFAULT 3,
    timeout_seconds INTEGER,
    scheduled_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,

    CONSTRAINT workflow_nodes_unique_key UNIQUE (workflow_id, node_key),
    CONSTRAINT fk_workflow_nodes_workflow FOREIGN KEY (workflow_id) REFERENCES workflows(id) ON DELETE CASCADE,
    CONSTRAINT workflow_nodes_status_check CHECK (status IN ('PENDING', 'READY', 'RUNNING', 'SUCCEEDED', 'FAILED', 'SKIPPED', 'CANCELLED')),
    CONSTRAINT workflow_nodes_attempts_check CHECK (max_attempts > 0),
    CONSTRAINT workflow_nodes_timeout_check CHECK (timeout_seconds IS NULL OR timeout_seconds > 0)
);

CREATE INDEX IF NOT EXISTS idx_workflow_nodes_status ON workflow_nodes(workflow_id, status);

-- 8. Workflow Edges table (DAG dependencies)
CREATE TABLE IF NOT EXISTS workflow_edges (
    workflow_id UUID NOT NULL,
    upstream_node_id UUID NOT NULL,
    downstream_node_id UUID NOT NULL,

    PRIMARY KEY (workflow_id, upstream_node_id, downstream_node_id),
    CONSTRAINT fk_edges_workflow FOREIGN KEY (workflow_id) REFERENCES workflows(id) ON DELETE CASCADE,
    CONSTRAINT fk_edges_upstream FOREIGN KEY (upstream_node_id) REFERENCES workflow_nodes(id) ON DELETE CASCADE,
    CONSTRAINT fk_edges_downstream FOREIGN KEY (downstream_node_id) REFERENCES workflow_nodes(id) ON DELETE CASCADE,
    CONSTRAINT workflow_edges_no_self_loop CHECK (upstream_node_id <> downstream_node_id)
);

CREATE INDEX IF NOT EXISTS idx_workflow_edges_upstream ON workflow_edges(upstream_node_id);
CREATE INDEX IF NOT EXISTS idx_workflow_edges_downstream ON workflow_edges(downstream_node_id);

-- 9. Jobs table
CREATE TABLE IF NOT EXISTS jobs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id UUID NOT NULL,
    workflow_id UUID,
    workflow_node_id UUID,
    queue_id UUID NOT NULL,
    worker_id UUID,
    task_type VARCHAR(100) NOT NULL,
    payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    priority INTEGER NOT NULL DEFAULT 0,
    status VARCHAR(30) NOT NULL DEFAULT 'PENDING',
    attempt_count INTEGER NOT NULL DEFAULT 0,
    max_attempts INTEGER NOT NULL DEFAULT 3,
    scheduled_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    timeout_seconds INTEGER,
    lease_expires_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    cancelled_at TIMESTAMPTZ,
    error_code VARCHAR(100),
    error_message TEXT,
    idempotency_key VARCHAR(255),

    CONSTRAINT jobs_status_check CHECK (status IN (
        'PENDING', 'QUEUED', 'RUNNING', 'SUCCEEDED', 'FAILED', 'RETRYING', 'DEAD', 'CANCELLED'
    )),
    CONSTRAINT jobs_attempt_count_check CHECK (attempt_count >= 0),
    CONSTRAINT jobs_max_attempts_check CHECK (max_attempts > 0),
    CONSTRAINT jobs_timeout_check CHECK (timeout_seconds IS NULL OR timeout_seconds > 0),
    CONSTRAINT fk_jobs_user FOREIGN KEY (user_id) REFERENCES users(id),
    CONSTRAINT fk_jobs_workflow FOREIGN KEY (workflow_id) REFERENCES workflows(id) ON DELETE SET NULL,
    CONSTRAINT fk_jobs_workflow_node FOREIGN KEY (workflow_node_id) REFERENCES workflow_nodes(id) ON DELETE SET NULL,
    CONSTRAINT fk_jobs_queue FOREIGN KEY (queue_id) REFERENCES queues(id),
    CONSTRAINT fk_jobs_worker FOREIGN KEY (worker_id) REFERENCES workers(id) ON DELETE SET NULL
);

CREATE INDEX IF NOT EXISTS idx_jobs_queue_ready ON jobs (
    queue_id,
    priority DESC,
    scheduled_at ASC,
    created_at ASC
) WHERE status IN ('PENDING', 'QUEUED', 'RETRYING');

CREATE INDEX IF NOT EXISTS idx_jobs_expired_leases ON jobs(lease_expires_at) WHERE status = 'RUNNING';
CREATE INDEX IF NOT EXISTS idx_jobs_worker ON jobs(worker_id, status);
CREATE INDEX IF NOT EXISTS idx_jobs_workflow ON jobs(workflow_id);
CREATE INDEX IF NOT EXISTS idx_jobs_user_created ON jobs(user_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_jobs_status_created ON jobs(status, created_at DESC);

-- 10. Job Attempts table
CREATE TABLE IF NOT EXISTS job_attempts (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    job_id UUID NOT NULL,
    attempt_number INTEGER NOT NULL,
    worker_id UUID,
    status VARCHAR(30) NOT NULL,
    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    finished_at TIMESTAMPTZ,
    lease_expires_at TIMESTAMPTZ,
    error_code VARCHAR(100),
    error_message TEXT,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,

    CONSTRAINT job_attempt_unique UNIQUE (job_id, attempt_number),
    CONSTRAINT job_attempts_status_check CHECK (status IN (
        'RUNNING', 'SUCCEEDED', 'FAILED', 'TIMEOUT', 'CANCELLED', 'ABANDONED'
    )),
    CONSTRAINT job_attempt_number_check CHECK (attempt_number > 0),
    CONSTRAINT fk_job_attempt_job FOREIGN KEY (job_id) REFERENCES jobs(id) ON DELETE CASCADE,
    CONSTRAINT fk_job_attempt_worker FOREIGN KEY (worker_id) REFERENCES workers(id) ON DELETE SET NULL
);

CREATE INDEX IF NOT EXISTS idx_job_attempts_job ON job_attempts(job_id, attempt_number);
CREATE INDEX IF NOT EXISTS idx_job_attempts_worker ON job_attempts(worker_id, started_at DESC);

-- 11. Idempotency Keys table
CREATE TABLE IF NOT EXISTS idempotency_keys (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id UUID NOT NULL,
    key VARCHAR(255) NOT NULL,
    request_hash VARCHAR(128) NOT NULL,
    resource_id UUID,
    resource_type VARCHAR(50),
    response_status INTEGER,
    response_body JSONB,
    expires_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT idempotency_user_key_unique UNIQUE (user_id, key),
    CONSTRAINT fk_idempotency_user FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_idempotency_expires ON idempotency_keys(expires_at);

-- 12. Outbox Events table
CREATE TABLE IF NOT EXISTS outbox_events (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    event_type VARCHAR(100) NOT NULL,
    aggregate_type VARCHAR(50) NOT NULL,
    aggregate_id UUID NOT NULL,
    payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    status VARCHAR(20) NOT NULL DEFAULT 'PENDING',
    attempts INTEGER NOT NULL DEFAULT 0,
    available_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    published_at TIMESTAMPTZ,

    CONSTRAINT outbox_status_check CHECK (status IN ('PENDING', 'PUBLISHED', 'FAILED'))
);

CREATE INDEX IF NOT EXISTS idx_outbox_events_pending ON outbox_events(status, available_at) WHERE status = 'PENDING';

-- 13. Audit Logs table
CREATE TABLE IF NOT EXISTS audit_logs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id UUID,
    job_id UUID,
    workflow_id UUID,
    worker_id UUID,
    action VARCHAR(100) NOT NULL,
    resource_type VARCHAR(50),
    resource_id UUID,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    request_id VARCHAR(128),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT fk_audit_user FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE SET NULL,
    CONSTRAINT fk_audit_job FOREIGN KEY (job_id) REFERENCES jobs(id) ON DELETE SET NULL,
    CONSTRAINT fk_audit_workflow FOREIGN KEY (workflow_id) REFERENCES workflows(id) ON DELETE SET NULL,
    CONSTRAINT fk_audit_worker FOREIGN KEY (worker_id) REFERENCES workers(id) ON DELETE SET NULL
);

CREATE INDEX IF NOT EXISTS idx_audit_logs_created ON audit_logs(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_audit_logs_request ON audit_logs(request_id);
CREATE INDEX IF NOT EXISTS idx_audit_logs_resource ON audit_logs(resource_type, resource_id);
