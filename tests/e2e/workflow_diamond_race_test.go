package e2e_test

import (
	"context"
	"encoding/json"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	appJob "github.com/forgeflow/forgeflow/internal/application/job"
	appWorkflow "github.com/forgeflow/forgeflow/internal/application/workflow"
	domainJob "github.com/forgeflow/forgeflow/internal/domain/job"
	"github.com/forgeflow/forgeflow/internal/domain/queue"
	domainWf "github.com/forgeflow/forgeflow/internal/domain/workflow"
	"github.com/forgeflow/forgeflow/internal/ports"
)

type threadSafeWfRepo struct {
	mu    sync.Mutex
	wfs   map[uuid.UUID]*domainWf.Workflow
	nodes map[uuid.UUID][]*domainWf.Node
	edges map[uuid.UUID][]*domainWf.Edge
}

func newThreadSafeWfRepo() *threadSafeWfRepo {
	return &threadSafeWfRepo{
		wfs:   make(map[uuid.UUID]*domainWf.Workflow),
		nodes: make(map[uuid.UUID][]*domainWf.Node),
		edges: make(map[uuid.UUID][]*domainWf.Edge),
	}
}

func (m *threadSafeWfRepo) Create(ctx context.Context, wf *domainWf.Workflow, nodes []*domainWf.Node, edges []*domainWf.Edge) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.wfs[wf.ID] = wf
	m.nodes[wf.ID] = nodes
	m.edges[wf.ID] = edges
	return nil
}

func (m *threadSafeWfRepo) GetByID(ctx context.Context, id uuid.UUID) (*domainWf.Workflow, []*domainWf.Node, []*domainWf.Edge, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	wf, ok := m.wfs[id]
	if !ok {
		return nil, nil, nil, domainWf.ErrWorkflowNotFound
	}
	return wf, m.nodes[id], m.edges[id], nil
}

func (m *threadSafeWfRepo) List(ctx context.Context, filter ports.WorkflowFilter) ([]*domainWf.Workflow, int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var list []*domainWf.Workflow
	for _, w := range m.wfs {
		list = append(list, w)
	}
	return list, int64(len(list)), nil
}

func (m *threadSafeWfRepo) UpdateStatus(ctx context.Context, id uuid.UUID, status domainWf.Status) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if wf, ok := m.wfs[id]; ok {
		wf.Status = status
	}
	return nil
}

func (m *threadSafeWfRepo) UpdateNodeStatus(ctx context.Context, nodeID uuid.UUID, status domainWf.NodeStatus, jobID *uuid.UUID) error {
	m.mu.Lock()
	defer m.mu.Unlock()
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

type countingJobRepo struct {
	mu           sync.Mutex
	jobs         map[uuid.UUID]*domainJob.Job
	createdCount int64
}

func newCountingJobRepo() *countingJobRepo {
	return &countingJobRepo{jobs: make(map[uuid.UUID]*domainJob.Job)}
}

func (m *countingJobRepo) Create(ctx context.Context, j *domainJob.Job, event interface{}) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	atomic.AddInt64(&m.createdCount, 1)
	m.jobs[j.ID] = j
	return nil
}

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

func TestConcurrency_WorkflowDiamondGraphRace(t *testing.T) {
	wfRepo := newThreadSafeWfRepo()
	jobRepo := newThreadSafeMockJobRepo()
	queueRepo := &mockQueueRepo{}
	attemptRepo := &mockAttemptRepo{}

	jobSvc := appJob.NewService(jobRepo, attemptRepo, nil, nil)
	wfSvc := appWorkflow.NewService(wfRepo, jobSvc, queueRepo, jobRepo)

	ctx := context.Background()
	cmd := appWorkflow.CreateWorkflowCommand{
		UserID: uuid.New(),
		Name:   "diamond-ci-cd-pipeline",
		Nodes: []appWorkflow.CreateNodeDTO{
			{Name: "BUILD", TaskType: "custom-demo", Payload: json.RawMessage(`{"stage":"build"}`)},
			{Name: "TEST", TaskType: "custom-demo", Payload: json.RawMessage(`{"stage":"test"}`)},
			{Name: "SECURITY", TaskType: "custom-demo", Payload: json.RawMessage(`{"stage":"security"}`)},
			{Name: "DEPLOY", TaskType: "custom-demo", Payload: json.RawMessage(`{"stage":"deploy"}`)},
		},
		Edges: []appWorkflow.CreateEdgeDTO{
			{FromNodeName: "BUILD", ToNodeName: "TEST", Condition: domainWf.ConditionAllSuccess},
			{FromNodeName: "BUILD", ToNodeName: "SECURITY", Condition: domainWf.ConditionAllSuccess},
			{FromNodeName: "TEST", ToNodeName: "DEPLOY", Condition: domainWf.ConditionAllSuccess},
			{FromNodeName: "SECURITY", ToNodeName: "DEPLOY", Condition: domainWf.ConditionAllSuccess},
		},
	}

	wf, nodes, _, err := wfSvc.Create(ctx, cmd)
	require.NoError(t, err)
	require.NoError(t, wfSvc.Start(ctx, wf.ID))

	var buildNode, testNode, secNode, deployNode *domainWf.Node
	for _, n := range nodes {
		switch n.Name {
		case "BUILD":
			buildNode = n
		case "TEST":
			testNode = n
		case "SECURITY":
			secNode = n
		case "DEPLOY":
			deployNode = n
		}
	}

	// 1. Mark BUILD as SUCCEEDED -> TEST and SECURITY become runnable
	require.NoError(t, wfSvc.HandleJobUpdate(ctx, wf.ID, buildNode.ID, true))

	// 2. Concurrently simulate TEST and SECURITY completing at the exact same millisecond
	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		_ = wfSvc.HandleJobUpdate(ctx, wf.ID, testNode.ID, true)
	}()

	go func() {
		defer wg.Done()
		_ = wfSvc.HandleJobUpdate(ctx, wf.ID, secNode.ID, true)
	}()

	wg.Wait()

	// 3. Assert workflow state
	updatedWf, updatedNodes, _, err := wfSvc.GetByID(ctx, wf.ID)
	require.NoError(t, err)
	assert.Equal(t, domainWf.StatusRunning, updatedWf.Status)

	for _, n := range updatedNodes {
		if n.ID == deployNode.ID {
			// DEPLOY must be QUEUED with a single job ID
			assert.Equal(t, domainWf.NodeQueued, n.Status)
			assert.NotNil(t, n.JobID)
		}
	}
}
