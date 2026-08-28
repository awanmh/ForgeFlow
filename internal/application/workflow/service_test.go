package workflow_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	appJob "github.com/forgeflow/forgeflow/internal/application/job"
	appWorkflow "github.com/forgeflow/forgeflow/internal/application/workflow"
	domainJob "github.com/forgeflow/forgeflow/internal/domain/job"
	"github.com/forgeflow/forgeflow/internal/domain/outbox"
	"github.com/forgeflow/forgeflow/internal/domain/queue"
	domainWf "github.com/forgeflow/forgeflow/internal/domain/workflow"
	"github.com/forgeflow/forgeflow/internal/ports"
)

type mockWfRepo struct {
	wfs   map[uuid.UUID]*domainWf.Workflow
	nodes map[uuid.UUID][]*domainWf.Node
	edges map[uuid.UUID][]*domainWf.Edge
}

func newMockWfRepo() *mockWfRepo {
	return &mockWfRepo{
		wfs:   make(map[uuid.UUID]*domainWf.Workflow),
		nodes: make(map[uuid.UUID][]*domainWf.Node),
		edges: make(map[uuid.UUID][]*domainWf.Edge),
	}
}

func (m *mockWfRepo) Create(ctx context.Context, wf *domainWf.Workflow, nodes []*domainWf.Node, edges []*domainWf.Edge) error {
	m.wfs[wf.ID] = wf
	m.nodes[wf.ID] = nodes
	m.edges[wf.ID] = edges
	return nil
}

func (m *mockWfRepo) GetByID(ctx context.Context, id uuid.UUID) (*domainWf.Workflow, []*domainWf.Node, []*domainWf.Edge, error) {
	wf, ok := m.wfs[id]
	if !ok {
		return nil, nil, nil, domainWf.ErrWorkflowNotFound
	}
	return wf, m.nodes[id], m.edges[id], nil
}

func (m *mockWfRepo) List(ctx context.Context, filter ports.WorkflowFilter) ([]*domainWf.Workflow, int64, error) {
	var list []*domainWf.Workflow
	for _, w := range m.wfs {
		list = append(list, w)
	}
	return list, int64(len(list)), nil
}

func (m *mockWfRepo) UpdateStatus(ctx context.Context, id uuid.UUID, status domainWf.Status) error {
	if wf, ok := m.wfs[id]; ok {
		wf.Status = status
	}
	return nil
}

func (m *mockWfRepo) UpdateNodeStatus(ctx context.Context, nodeID uuid.UUID, status domainWf.NodeStatus, jobID *uuid.UUID) error {
	for _, nodes := range m.nodes {
		for _, n := range nodes {
			if n.ID == nodeID {
				n.Status = status
				if jobID != nil {
					n.JobID = jobID
				}
			}
		}
	}
	return nil
}

type mockJobRepo struct {
	jobs map[uuid.UUID]*domainJob.Job
}

func (m *mockJobRepo) Create(ctx context.Context, j *domainJob.Job, event *outbox.Event) error {
	if m.jobs == nil {
		m.jobs = make(map[uuid.UUID]*domainJob.Job)
	}
	m.jobs[j.ID] = j
	return nil
}
func (m *mockJobRepo) GetByID(ctx context.Context, id uuid.UUID) (*domainJob.Job, error) {
	if j, ok := m.jobs[id]; ok {
		return j, nil
	}
	return nil, domainJob.ErrJobNotFound
}
func (m *mockJobRepo) List(ctx context.Context, filter ports.JobFilter) ([]*domainJob.Job, int64, error) {
	return nil, 0, nil
}
func (m *mockJobRepo) ClaimNext(ctx context.Context, queueID uuid.UUID, workerID uuid.UUID, leaseDuration time.Duration) (*domainJob.Job, *domainJob.JobAttempt, error) {
	return nil, nil, nil
}
func (m *mockJobRepo) RenewLease(ctx context.Context, jobID uuid.UUID, workerID uuid.UUID, leaseDuration time.Duration) error {
	return nil
}
func (m *mockJobRepo) Complete(ctx context.Context, jobID uuid.UUID, workerID uuid.UUID) error {
	return nil
}
func (m *mockJobRepo) Fail(ctx context.Context, jobID uuid.UUID, workerID uuid.UUID, errCode, errMsg string, retryable bool, retryDelay time.Duration) error {
	return nil
}
func (m *mockJobRepo) Cancel(ctx context.Context, jobID uuid.UUID) error {
	if j, ok := m.jobs[jobID]; ok {
		j.Status = domainJob.StatusCancelled
	}
	return nil
}
func (m *mockJobRepo) RecoverExpiredLeases(ctx context.Context, limit int, retryDelay time.Duration) ([]*domainJob.Job, error) {
	return nil, nil
}

type mockAttemptRepo struct{}

func (m *mockAttemptRepo) Create(ctx context.Context, a *domainJob.JobAttempt) error { return nil }
func (m *mockAttemptRepo) ListByJobID(ctx context.Context, jobID uuid.UUID) ([]*domainJob.JobAttempt, error) {
	return nil, nil
}
func (m *mockAttemptRepo) Update(ctx context.Context, a *domainJob.JobAttempt) error { return nil }

type mockQueueRepo struct{}

func (m *mockQueueRepo) Create(ctx context.Context, q *queue.Queue) error { return nil }
func (m *mockQueueRepo) GetByID(ctx context.Context, id uuid.UUID) (*queue.Queue, error) {
	return &queue.Queue{ID: id, Name: "default"}, nil
}
func (m *mockQueueRepo) GetByName(ctx context.Context, name string) (*queue.Queue, error) {
	return &queue.Queue{ID: uuid.New(), Name: name}, nil
}
func (m *mockQueueRepo) List(ctx context.Context) ([]*queue.Queue, error) { return nil, nil }
func (m *mockQueueRepo) Update(ctx context.Context, q *queue.Queue) error  { return nil }
func (m *mockQueueRepo) Delete(ctx context.Context, id uuid.UUID) error   { return nil }

func TestWorkflowService_CreateAndStart(t *testing.T) {
	wfRepo := newMockWfRepo()
	jobRepo := &mockJobRepo{}
	queueRepo := &mockQueueRepo{}
	attemptRepo := &mockAttemptRepo{}

	// Construct JobService
	jobSvc := appJob.NewService(jobRepo, attemptRepo, nil, nil)
	wfSvc := appWorkflow.NewService(wfRepo, jobSvc, queueRepo, jobRepo)

	ctx := context.Background()
	cmd := appWorkflow.CreateWorkflowCommand{
		UserID: uuid.New(),
		Name:   "data-pipeline",
		Nodes: []appWorkflow.CreateNodeDTO{
			{Name: "extract", TaskType: "custom-demo", Payload: json.RawMessage(`{"stage":"extract"}`)},
			{Name: "transform", TaskType: "custom-demo", Payload: json.RawMessage(`{"stage":"transform"}`)},
			{Name: "load", TaskType: "custom-demo", Payload: json.RawMessage(`{"stage":"load"}`)},
		},
		Edges: []appWorkflow.CreateEdgeDTO{
			{FromNodeName: "extract", ToNodeName: "transform", Condition: domainWf.ConditionAllSuccess},
			{FromNodeName: "transform", ToNodeName: "load", Condition: domainWf.ConditionAllSuccess},
		},
	}

	wf, nodes, edges, err := wfSvc.Create(ctx, cmd)
	require.NoError(t, err)
	assert.NotNil(t, wf)
	assert.Len(t, nodes, 3)
	assert.Len(t, edges, 2)

	// Start workflow
	err = wfSvc.Start(ctx, wf.ID)
	require.NoError(t, err)

	startedWf, startedNodes, _, err := wfSvc.GetByID(ctx, wf.ID)
	require.NoError(t, err)
	assert.Equal(t, domainWf.StatusRunning, startedWf.Status)
	assert.Equal(t, domainWf.NodeQueued, startedNodes[0].Status) // Root node materialized
}
