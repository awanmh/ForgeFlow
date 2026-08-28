package workflow

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"

	appJob "github.com/forgeflow/forgeflow/internal/application/job"
	"github.com/forgeflow/forgeflow/internal/domain/workflow"
	"github.com/forgeflow/forgeflow/internal/ports"
)

type CreateNodeDTO struct {
	Name           string          `json:"name" binding:"required"`
	TaskType       string          `json:"task_type" binding:"required"`
	Payload        json.RawMessage `json:"payload"`
	Priority       int             `json:"priority"`
	MaxAttempts    int             `json:"max_attempts"`
	TimeoutSeconds *int            `json:"timeout_seconds"`
}

type CreateEdgeDTO struct {
	FromNodeName string                 `json:"from_node_name" binding:"required"`
	ToNodeName   string                 `json:"to_node_name" binding:"required"`
	Condition    workflow.EdgeCondition `json:"condition"`
}

type CreateWorkflowCommand struct {
	UserID      uuid.UUID       `json:"user_id"`
	Name        string          `json:"name"`
	Description *string         `json:"description,omitempty"`
	Nodes       []CreateNodeDTO `json:"nodes"`
	Edges       []CreateEdgeDTO `json:"edges"`
}

// Service coordinates DAG validation, persistence, node materialization, and workflow execution.
type Service struct {
	mu        sync.Mutex
	wfRepo    ports.WorkflowRepository
	jobSvc    *appJob.Service
	queueRepo ports.QueueRepository
	jobRepo   ports.JobRepository
}

// NewService constructs a new Workflow application service.
func NewService(wfRepo ports.WorkflowRepository, jobSvc *appJob.Service, queueRepo ports.QueueRepository, jobRepo ports.JobRepository) *Service {
	return &Service{
		wfRepo:    wfRepo,
		jobSvc:    jobSvc,
		queueRepo: queueRepo,
		jobRepo:   jobRepo,
	}
}

// Create validates DAG cycle invariants and persists the workflow definition.
func (s *Service) Create(ctx context.Context, cmd CreateWorkflowCommand) (*workflow.Workflow, []*workflow.Node, []*workflow.Edge, error) {
	if cmd.Name == "" {
		return nil, nil, nil, errors.New("workflow name is required")
	}
	if len(cmd.Nodes) == 0 {
		return nil, nil, nil, workflow.ErrEmptyWorkflow
	}

	wfID := uuid.New()
	now := time.Now().UTC()

	wf := &workflow.Workflow{
		ID:          wfID,
		UserID:      cmd.UserID,
		Name:        cmd.Name,
		Description: cmd.Description,
		Status:      workflow.StatusPending,
		CreatedAt:   now,
		UpdatedAt:   now,
	}

	nameToID := make(map[string]uuid.UUID, len(cmd.Nodes))
	var nodes []*workflow.Node

	for _, n := range cmd.Nodes {
		nodeID := uuid.New()
		nameToID[n.Name] = nodeID

		maxAtt := n.MaxAttempts
		if maxAtt <= 0 {
			maxAtt = 3
		}
		payload := n.Payload
		if len(payload) == 0 {
			payload = []byte("{}")
		}

		node := &workflow.Node{
			ID:             nodeID,
			WorkflowID:     wfID,
			Name:           n.Name,
			TaskType:       n.TaskType,
			Payload:        payload,
			Priority:       n.Priority,
			MaxAttempts:    maxAtt,
			TimeoutSeconds: n.TimeoutSeconds,
			Status:         workflow.NodePending,
			CreatedAt:      now,
			UpdatedAt:      now,
		}
		nodes = append(nodes, node)
	}

	var edges []*workflow.Edge
	for _, e := range cmd.Edges {
		fromID, fromOk := nameToID[e.FromNodeName]
		toID, toOk := nameToID[e.ToNodeName]
		if !fromOk || !toOk {
			return nil, nil, nil, fmt.Errorf("%w: invalid edge between %s and %s", workflow.ErrNodeNotFound, e.FromNodeName, e.ToNodeName)
		}

		cond := e.Condition
		if cond == "" {
			cond = workflow.ConditionAllSuccess
		}

		edge := &workflow.Edge{
			ID:         uuid.New(),
			WorkflowID: wfID,
			FromNodeID: fromID,
			ToNodeID:   toID,
			Condition:  cond,
		}
		edges = append(edges, edge)
	}

	// Validate DAG structure and detect cycles
	if _, err := workflow.NewDAG(wf, nodes, edges); err != nil {
		return nil, nil, nil, err
	}

	if err := s.wfRepo.Create(ctx, wf, nodes, edges); err != nil {
		return nil, nil, nil, fmt.Errorf("failed to persist workflow: %w", err)
	}

	return wf, nodes, edges, nil
}

// Start initiates workflow execution by materializing and enqueuing root nodes.
func (s *Service) Start(ctx context.Context, workflowID uuid.UUID) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	wf, nodes, edges, err := s.wfRepo.GetByID(ctx, workflowID)
	if err != nil {
		return err
	}

	if wf.Status != workflow.StatusPending && wf.Status != workflow.StatusPaused {
		return fmt.Errorf("%w: cannot start workflow in status %s", workflow.ErrInvalidStateTransition, wf.Status)
	}

	dag, err := workflow.NewDAG(wf, nodes, edges)
	if err != nil {
		return err
	}

	if err := s.wfRepo.UpdateStatus(ctx, workflowID, workflow.StatusRunning); err != nil {
		return fmt.Errorf("failed to update workflow status to RUNNING: %w", err)
	}

	// Materialize runnable root nodes
	roots := dag.RootNodes()
	for _, root := range roots {
		if err := s.materializeNode(ctx, root); err != nil {
			return fmt.Errorf("failed to materialize root node %s: %w", root.Name, err)
		}
	}

	return nil
}

// HandleJobUpdate is called when a job belonging to a workflow finishes execution.
func (s *Service) HandleJobUpdate(ctx context.Context, workflowID, workflowNodeID uuid.UUID, succeeded bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	wf, nodes, edges, err := s.wfRepo.GetByID(ctx, workflowID)
	if err != nil {
		return err
	}

	nodeStatus := workflow.NodeSucceeded
	if !succeeded {
		nodeStatus = workflow.NodeFailed
	}

	if err := s.wfRepo.UpdateNodeStatus(ctx, workflowNodeID, nodeStatus, nil); err != nil {
		return fmt.Errorf("failed to update node status: %w", err)
	}

	// Refresh node state
	for _, n := range nodes {
		if n.ID == workflowNodeID {
			n.Status = nodeStatus
		}
	}

	dag, err := workflow.NewDAG(wf, nodes, edges)
	if err != nil {
		return err
	}

	// Materialize any downstream nodes whose dependency conditions are now met
	for _, n := range nodes {
		if n.Status == workflow.NodePending && dag.IsNodeRunnable(n.ID) {
			_ = s.materializeNode(ctx, n)
		}
	}

	// Check if entire workflow has finished
	if dag.IsComplete() {
		finalStatus := workflow.StatusSucceeded
		if dag.HasFailures() {
			finalStatus = workflow.StatusFailed
		}
		_ = s.wfRepo.UpdateStatus(ctx, workflowID, finalStatus)
	}

	return nil
}

func (s *Service) materializeNode(ctx context.Context, node *workflow.Node) error {
	if node.Status != workflow.NodePending {
		return nil
	}

	defaultQueueID := uuid.MustParse("00000000-0000-0000-0000-000000000002")

	cmd := appJob.SubmitJobCommand{
		UserID:         uuid.MustParse("00000000-0000-0000-0000-000000000001"),
		QueueID:        defaultQueueID,
		QueueName:      "default",
		TaskType:       node.TaskType,
		Payload:        node.Payload,
		Priority:       node.Priority,
		MaxAttempts:    node.MaxAttempts,
		ScheduledAt:    time.Now().UTC(),
		TimeoutSeconds: node.TimeoutSeconds,
	}

	j, _, _, err := s.jobSvc.Submit(ctx, cmd)
	if err != nil {
		return err
	}

	node.Status = workflow.NodeQueued
	node.JobID = &j.ID
	return s.wfRepo.UpdateNodeStatus(ctx, node.ID, workflow.NodeQueued, &j.ID)
}

func (s *Service) GetByID(ctx context.Context, id uuid.UUID) (*workflow.Workflow, []*workflow.Node, []*workflow.Edge, error) {
	return s.wfRepo.GetByID(ctx, id)
}

func (s *Service) List(ctx context.Context, filter ports.WorkflowFilter) ([]*workflow.Workflow, int64, error) {
	return s.wfRepo.List(ctx, filter)
}

func (s *Service) Cancel(ctx context.Context, id uuid.UUID) error {
	return s.wfRepo.UpdateStatus(ctx, id, workflow.StatusCancelled)
}

type domainJobFilter = workflow.Workflow
