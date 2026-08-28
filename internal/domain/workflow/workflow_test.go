package workflow_test

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/forgeflow/forgeflow/internal/domain/workflow"
)

func TestWorkflowDAG_CycleDetection(t *testing.T) {
	wfID := uuid.New()
	wf := &workflow.Workflow{
		ID:        wfID,
		UserID:    uuid.New(),
		Name:      "test-workflow",
		Status:    workflow.StatusPending,
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}

	t.Run("valid linear DAG: A -> B -> C", func(t *testing.T) {
		nodeA := &workflow.Node{ID: uuid.New(), WorkflowID: wfID, Name: "A", Status: workflow.NodePending}
		nodeB := &workflow.Node{ID: uuid.New(), WorkflowID: wfID, Name: "B", Status: workflow.NodePending}
		nodeC := &workflow.Node{ID: uuid.New(), WorkflowID: wfID, Name: "C", Status: workflow.NodePending}

		edges := []*workflow.Edge{
			{ID: uuid.New(), WorkflowID: wfID, FromNodeID: nodeA.ID, ToNodeID: nodeB.ID, Condition: workflow.ConditionAllSuccess},
			{ID: uuid.New(), WorkflowID: wfID, FromNodeID: nodeB.ID, ToNodeID: nodeC.ID, Condition: workflow.ConditionAllSuccess},
		}

		dag, err := workflow.NewDAG(wf, []*workflow.Node{nodeA, nodeB, nodeC}, edges)
		require.NoError(t, err)
		assert.NotNil(t, dag)

		roots := dag.RootNodes()
		assert.Len(t, roots, 1)
		assert.Equal(t, nodeA.ID, roots[0].ID)

		assert.True(t, dag.IsNodeRunnable(nodeA.ID))
		assert.False(t, dag.IsNodeRunnable(nodeB.ID))
		assert.False(t, dag.IsNodeRunnable(nodeC.ID))

		// Mark nodeA SUCCEEDED
		nodeA.Status = workflow.NodeSucceeded
		assert.True(t, dag.IsNodeRunnable(nodeB.ID))
		assert.False(t, dag.IsNodeRunnable(nodeC.ID))

		// Mark nodeB SUCCEEDED
		nodeB.Status = workflow.NodeSucceeded
		assert.True(t, dag.IsNodeRunnable(nodeC.ID))
	})

	t.Run("cycle detection: A -> B -> C -> A", func(t *testing.T) {
		nodeA := &workflow.Node{ID: uuid.New(), WorkflowID: wfID, Name: "A", Status: workflow.NodePending}
		nodeB := &workflow.Node{ID: uuid.New(), WorkflowID: wfID, Name: "B", Status: workflow.NodePending}
		nodeC := &workflow.Node{ID: uuid.New(), WorkflowID: wfID, Name: "C", Status: workflow.NodePending}

		edges := []*workflow.Edge{
			{ID: uuid.New(), WorkflowID: wfID, FromNodeID: nodeA.ID, ToNodeID: nodeB.ID, Condition: workflow.ConditionAllSuccess},
			{ID: uuid.New(), WorkflowID: wfID, FromNodeID: nodeB.ID, ToNodeID: nodeC.ID, Condition: workflow.ConditionAllSuccess},
			{ID: uuid.New(), WorkflowID: wfID, FromNodeID: nodeC.ID, ToNodeID: nodeA.ID, Condition: workflow.ConditionAllSuccess},
		}

		_, err := workflow.NewDAG(wf, []*workflow.Node{nodeA, nodeB, nodeC}, edges)
		require.Error(t, err)
		assert.ErrorIs(t, err, workflow.ErrCycleDetected)
	})

	t.Run("self-referential edge: A -> A", func(t *testing.T) {
		nodeA := &workflow.Node{ID: uuid.New(), WorkflowID: wfID, Name: "A", Status: workflow.NodePending}
		edges := []*workflow.Edge{
			{ID: uuid.New(), WorkflowID: wfID, FromNodeID: nodeA.ID, ToNodeID: nodeA.ID, Condition: workflow.ConditionAllSuccess},
		}

		_, err := workflow.NewDAG(wf, []*workflow.Node{nodeA}, edges)
		require.Error(t, err)
		assert.ErrorIs(t, err, workflow.ErrSelfReferentialEdge)
	})

	t.Run("diamond graph: A -> (B, C) -> D", func(t *testing.T) {
		nodeA := &workflow.Node{ID: uuid.New(), WorkflowID: wfID, Name: "A", Status: workflow.NodePending}
		nodeB := &workflow.Node{ID: uuid.New(), WorkflowID: wfID, Name: "B", Status: workflow.NodePending}
		nodeC := &workflow.Node{ID: uuid.New(), WorkflowID: wfID, Name: "C", Status: workflow.NodePending}
		nodeD := &workflow.Node{ID: uuid.New(), WorkflowID: wfID, Name: "D", Status: workflow.NodePending}

		edges := []*workflow.Edge{
			{ID: uuid.New(), WorkflowID: wfID, FromNodeID: nodeA.ID, ToNodeID: nodeB.ID, Condition: workflow.ConditionAllSuccess},
			{ID: uuid.New(), WorkflowID: wfID, FromNodeID: nodeA.ID, ToNodeID: nodeC.ID, Condition: workflow.ConditionAllSuccess},
			{ID: uuid.New(), WorkflowID: wfID, FromNodeID: nodeB.ID, ToNodeID: nodeD.ID, Condition: workflow.ConditionAllSuccess},
			{ID: uuid.New(), WorkflowID: wfID, FromNodeID: nodeC.ID, ToNodeID: nodeD.ID, Condition: workflow.ConditionAllSuccess},
		}

		dag, err := workflow.NewDAG(wf, []*workflow.Node{nodeA, nodeB, nodeC, nodeD}, edges)
		require.NoError(t, err)

		nodeA.Status = workflow.NodeSucceeded
		assert.True(t, dag.IsNodeRunnable(nodeB.ID))
		assert.True(t, dag.IsNodeRunnable(nodeC.ID))
		assert.False(t, dag.IsNodeRunnable(nodeD.ID))

		// Only B completes -> D is still not runnable
		nodeB.Status = workflow.NodeSucceeded
		assert.False(t, dag.IsNodeRunnable(nodeD.ID))

		// Both B and C complete -> D becomes runnable
		nodeC.Status = workflow.NodeSucceeded
		assert.True(t, dag.IsNodeRunnable(nodeD.ID))
	})
}
