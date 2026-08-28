package postgres

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/forgeflow/forgeflow/internal/domain/workflow"
	"github.com/forgeflow/forgeflow/internal/ports"
)

type WorkflowRepo struct {
	client *Client
}

func NewWorkflowRepo(client *Client) *WorkflowRepo {
	return &WorkflowRepo{client: client}
}

func (r *WorkflowRepo) Create(ctx context.Context, wf *workflow.Workflow, nodes []*workflow.Node, edges []*workflow.Edge) error {
	return r.client.WithinTransaction(ctx, func(ctx context.Context, tx pgx.Tx) error {
		wfQuery := `
			INSERT INTO workflows (id, user_id, name, description, status, created_at, updated_at)
			VALUES ($1, $2, $3, $4, $5, $6, $7)
		`
		_, err := tx.Exec(ctx, wfQuery,
			wf.ID, wf.UserID, wf.Name, wf.Description, string(wf.Status), wf.CreatedAt, wf.UpdatedAt,
		)
		if err != nil {
			return fmt.Errorf("failed to insert workflow: %w", err)
		}

		nodeQuery := `
			INSERT INTO workflow_nodes (
				id, workflow_id, name, task_type, payload, priority, max_attempts,
				timeout_seconds, status, job_id, created_at, updated_at
			) VALUES (
				$1, $2, $3, $4, $5, $6, $7,
				$8, $9, $10, $11, $12
			)
		`
		for _, n := range nodes {
			_, err = tx.Exec(ctx, nodeQuery,
				n.ID, n.WorkflowID, n.Name, n.TaskType, n.Payload, n.Priority, n.MaxAttempts,
				n.TimeoutSeconds, string(n.Status), n.JobID, n.CreatedAt, n.UpdatedAt,
			)
			if err != nil {
				return fmt.Errorf("failed to insert workflow node %s: %w", n.Name, err)
			}
		}

		edgeQuery := `
			INSERT INTO workflow_edges (id, workflow_id, from_node_id, to_node_id, condition)
			VALUES ($1, $2, $3, $4, $5)
		`
		for _, e := range edges {
			_, err = tx.Exec(ctx, edgeQuery,
				e.ID, e.WorkflowID, e.FromNodeID, e.ToNodeID, string(e.Condition),
			)
			if err != nil {
				return fmt.Errorf("failed to insert workflow edge: %w", err)
			}
		}

		return nil
	})
}

func (r *WorkflowRepo) GetByID(ctx context.Context, id uuid.UUID) (*workflow.Workflow, []*workflow.Node, []*workflow.Edge, error) {
	wfQuery := `
		SELECT id, user_id, name, description, status, created_at, updated_at, started_at, completed_at, cancelled_at
		FROM workflows
		WHERE id = $1
	`
	wf := &workflow.Workflow{}
	var statusStr string
	err := r.client.Pool.QueryRow(ctx, wfQuery, id).Scan(
		&wf.ID, &wf.UserID, &wf.Name, &wf.Description, &statusStr,
		&wf.CreatedAt, &wf.UpdatedAt, &wf.StartedAt, &wf.CompletedAt, &wf.CancelledAt,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil, nil, workflow.ErrWorkflowNotFound
		}
		return nil, nil, nil, fmt.Errorf("failed to fetch workflow: %w", err)
	}
	wf.Status = workflow.Status(statusStr)

	// Fetch nodes
	nodeQuery := `
		SELECT id, workflow_id, name, task_type, payload, priority, max_attempts, timeout_seconds, status, job_id, created_at, updated_at
		FROM workflow_nodes
		WHERE workflow_id = $1
		ORDER BY created_at ASC
	`
	nodeRows, err := r.client.Pool.Query(ctx, nodeQuery, id)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("failed to fetch workflow nodes: %w", err)
	}
	defer nodeRows.Close()

	var nodes []*workflow.Node
	for nodeRows.Next() {
		n := &workflow.Node{}
		var nStatus string
		if err := nodeRows.Scan(
			&n.ID, &n.WorkflowID, &n.Name, &n.TaskType, &n.Payload, &n.Priority, &n.MaxAttempts,
			&n.TimeoutSeconds, &nStatus, &n.JobID, &n.CreatedAt, &n.UpdatedAt,
		); err != nil {
			return nil, nil, nil, err
		}
		n.Status = workflow.NodeStatus(nStatus)
		nodes = append(nodes, n)
	}
	nodeRows.Close()

	// Fetch edges
	edgeQuery := `
		SELECT id, workflow_id, from_node_id, to_node_id, condition
		FROM workflow_edges
		WHERE workflow_id = $1
	`
	edgeRows, err := r.client.Pool.Query(ctx, edgeQuery, id)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("failed to fetch workflow edges: %w", err)
	}
	defer edgeRows.Close()

	var edges []*workflow.Edge
	for edgeRows.Next() {
		e := &workflow.Edge{}
		var condStr string
		if err := edgeRows.Scan(&e.ID, &e.WorkflowID, &e.FromNodeID, &e.ToNodeID, &condStr); err != nil {
			return nil, nil, nil, err
		}
		e.Condition = workflow.EdgeCondition(condStr)
		edges = append(edges, e)
	}

	return wf, nodes, edges, nil
}

func (r *WorkflowRepo) List(ctx context.Context, filter ports.WorkflowFilter) ([]*workflow.Workflow, int64, error) {
	var whereClauses []string
	var args []any
	argIdx := 1

	if filter.UserID != nil {
		whereClauses = append(whereClauses, fmt.Sprintf("user_id = $%d", argIdx))
		args = append(args, *filter.UserID)
		argIdx++
	}
	if filter.Status != nil {
		whereClauses = append(whereClauses, fmt.Sprintf("status = $%d", argIdx))
		args = append(args, string(*filter.Status))
		argIdx++
	}

	whereSQL := ""
	if len(whereClauses) > 0 {
		whereSQL = "WHERE " + strings.Join(whereClauses, " AND ")
	}

	var total int64
	countQuery := fmt.Sprintf("SELECT COUNT(*) FROM workflows %s", whereSQL)
	if err := r.client.Pool.QueryRow(ctx, countQuery, args...).Scan(&total); err != nil {
		return nil, 0, fmt.Errorf("failed to count workflows: %w", err)
	}

	limit := 50
	if filter.Limit > 0 && filter.Limit <= 100 {
		limit = filter.Limit
	}
	offset := 0
	if filter.Offset > 0 {
		offset = filter.Offset
	}

	listQuery := fmt.Sprintf(`
		SELECT id, user_id, name, description, status, created_at, updated_at, started_at, completed_at, cancelled_at
		FROM workflows
		%s
		ORDER BY created_at DESC
		LIMIT $%d OFFSET $%d
	`, whereSQL, argIdx, argIdx+1)

	args = append(args, limit, offset)
	rows, err := r.client.Pool.Query(ctx, listQuery, args...)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to list workflows: %w", err)
	}
	defer rows.Close()

	var workflows []*workflow.Workflow
	for rows.Next() {
		wf := &workflow.Workflow{}
		var statusStr string
		if err := rows.Scan(
			&wf.ID, &wf.UserID, &wf.Name, &wf.Description, &statusStr,
			&wf.CreatedAt, &wf.UpdatedAt, &wf.StartedAt, &wf.CompletedAt, &wf.CancelledAt,
		); err != nil {
			return nil, 0, err
		}
		wf.Status = workflow.Status(statusStr)
		workflows = append(workflows, wf)
	}

	return workflows, total, nil
}

func (r *WorkflowRepo) UpdateStatus(ctx context.Context, id uuid.UUID, status workflow.Status) error {
	now := time.Now().UTC()
	var startedAt, completedAt, cancelledAt *time.Time

	if status == workflow.StatusRunning {
		startedAt = &now
	} else if status == workflow.StatusSucceeded || status == workflow.StatusFailed {
		completedAt = &now
	} else if status == workflow.StatusCancelled {
		cancelledAt = &now
	}

	query := `
		UPDATE workflows
		SET status = $1,
		    started_at = COALESCE($2, started_at),
		    completed_at = COALESCE($3, completed_at),
		    cancelled_at = COALESCE($4, cancelled_at),
		    updated_at = $5
		WHERE id = $6
	`
	_, err := r.client.Pool.Exec(ctx, query, string(status), startedAt, completedAt, cancelledAt, now, id)
	if err != nil {
		return fmt.Errorf("failed to update workflow status: %w", err)
	}
	return nil
}

func (r *WorkflowRepo) UpdateNodeStatus(ctx context.Context, nodeID uuid.UUID, status workflow.NodeStatus, jobID *uuid.UUID) error {
	now := time.Now().UTC()
	query := `
		UPDATE workflow_nodes
		SET status = $1, job_id = COALESCE($2, job_id), updated_at = $3
		WHERE id = $4
	`
	_, err := r.client.Pool.Exec(ctx, query, string(status), jobID, now, nodeID)
	if err != nil {
		return fmt.Errorf("failed to update workflow node status: %w", err)
	}
	return nil
}
