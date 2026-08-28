package workflow

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
)

var (
	ErrCycleDetected          = errors.New("cycle detected in workflow DAG definition")
	ErrEmptyWorkflow          = errors.New("workflow must contain at least one node")
	ErrNodeNotFound           = errors.New("node referenced in edge does not exist")
	ErrSelfReferentialEdge    = errors.New("self-referential edges are not allowed")
	ErrWorkflowNotFound       = errors.New("workflow not found")
	ErrInvalidStateTransition = errors.New("invalid workflow state transition")
)

type Status string

const (
	StatusPending   Status = "PENDING"
	StatusRunning   Status = "RUNNING"
	StatusSucceeded Status = "SUCCEEDED"
	StatusFailed    Status = "FAILED"
	StatusCancelled Status = "CANCELLED"
	StatusPaused    Status = "PAUSED"
)

type NodeStatus string

const (
	NodePending   NodeStatus = "PENDING"
	NodeQueued    NodeStatus = "QUEUED"
	NodeRunning   NodeStatus = "RUNNING"
	NodeSucceeded NodeStatus = "SUCCEEDED"
	NodeFailed    NodeStatus = "FAILED"
	NodeCancelled NodeStatus = "CANCELLED"
	NodeSkipped   NodeStatus = "SKIPPED"
)

type EdgeCondition string

const (
	ConditionAllSuccess EdgeCondition = "ALL_SUCCESS"
	ConditionAnySuccess EdgeCondition = "ANY_SUCCESS"
	ConditionOnFailure  EdgeCondition = "ON_FAILURE"
	ConditionAlways     EdgeCondition = "ALWAYS"
)

// Workflow represents a Directed Acyclic Graph (DAG) of asynchronous tasks.
type Workflow struct {
	ID          uuid.UUID  `json:"id"`
	UserID      uuid.UUID  `json:"user_id"`
	Name        string     `json:"name"`
	Description *string    `json:"description,omitempty"`
	Status      Status     `json:"status"`
	CreatedAt   time.Time  `json:"created_at"`
	UpdatedAt   time.Time  `json:"updated_at"`
	StartedAt   *time.Time `json:"started_at,omitempty"`
	CompletedAt *time.Time `json:"completed_at,omitempty"`
	CancelledAt *time.Time `json:"cancelled_at,omitempty"`
}

// Node represents a vertex in the workflow DAG corresponding to an executable task.
type Node struct {
	ID             uuid.UUID       `json:"id"`
	WorkflowID     uuid.UUID       `json:"workflow_id"`
	Name           string          `json:"name"`
	TaskType       string          `json:"task_type"`
	Payload        json.RawMessage `json:"payload"`
	Priority       int             `json:"priority"`
	MaxAttempts    int             `json:"max_attempts"`
	TimeoutSeconds *int            `json:"timeout_seconds,omitempty"`
	Status         NodeStatus      `json:"status"`
	JobID          *uuid.UUID      `json:"job_id,omitempty"`
	CreatedAt      time.Time       `json:"created_at"`
	UpdatedAt      time.Time       `json:"updated_at"`
}

// Edge represents a directed dependency link between two workflow nodes.
type Edge struct {
	ID         uuid.UUID     `json:"id"`
	WorkflowID uuid.UUID     `json:"workflow_id"`
	FromNodeID uuid.UUID     `json:"from_node_id"`
	ToNodeID   uuid.UUID     `json:"to_node_id"`
	Condition  EdgeCondition `json:"condition"`
}

// DAG encapsulates a verified workflow structure with nodes and adjacency relationships.
type DAG struct {
	Workflow *Workflow
	Nodes    map[uuid.UUID]*Node
	Edges    []*Edge
	InDegree map[uuid.UUID]int
	AdjList  map[uuid.UUID][]uuid.UUID
	RevAdj   map[uuid.UUID][]uuid.UUID
}

// NewDAG validates the graph structure, executes Kahn's Algorithm for cycle detection, and builds adjacency lookups.
func NewDAG(wf *Workflow, nodes []*Node, edges []*Edge) (*DAG, error) {
	if len(nodes) == 0 {
		return nil, ErrEmptyWorkflow
	}

	nodeMap := make(map[uuid.UUID]*Node, len(nodes))
	inDegree := make(map[uuid.UUID]int, len(nodes))
	adjList := make(map[uuid.UUID][]uuid.UUID, len(nodes))
	revAdj := make(map[uuid.UUID][]uuid.UUID, len(nodes))

	for _, n := range nodes {
		nodeMap[n.ID] = n
		inDegree[n.ID] = 0
		adjList[n.ID] = nil
		revAdj[n.ID] = nil
	}

	for _, e := range edges {
		if e.FromNodeID == e.ToNodeID {
			return nil, fmt.Errorf("%w: node %s links to itself", ErrSelfReferentialEdge, e.FromNodeID)
		}
		if _, ok := nodeMap[e.FromNodeID]; !ok {
			return nil, fmt.Errorf("%w: from_node_id %s", ErrNodeNotFound, e.FromNodeID)
		}
		if _, ok := nodeMap[e.ToNodeID]; !ok {
			return nil, fmt.Errorf("%w: to_node_id %s", ErrNodeNotFound, e.ToNodeID)
		}

		adjList[e.FromNodeID] = append(adjList[e.FromNodeID], e.ToNodeID)
		revAdj[e.ToNodeID] = append(revAdj[e.ToNodeID], e.FromNodeID)
		inDegree[e.ToNodeID]++
	}

	// Kahn's Algorithm for Cycle Detection & Topological Sort
	queue := make([]uuid.UUID, 0, len(nodes))
	for nodeID, deg := range inDegree {
		if deg == 0 {
			queue = append(queue, nodeID)
		}
	}

	visitedCount := 0
	inDegreeCopy := make(map[uuid.UUID]int, len(inDegree))
	for k, v := range inDegree {
		inDegreeCopy[k] = v
	}

	for len(queue) > 0 {
		curr := queue[0]
		queue = queue[1:]
		visitedCount++

		for _, neighbor := range adjList[curr] {
			inDegreeCopy[neighbor]--
			if inDegreeCopy[neighbor] == 0 {
				queue = append(queue, neighbor)
			}
		}
	}

	if visitedCount != len(nodes) {
		return nil, fmt.Errorf("%w: graph contains circular dependencies", ErrCycleDetected)
	}

	return &DAG{
		Workflow: wf,
		Nodes:    nodeMap,
		Edges:    edges,
		InDegree: inDegree,
		AdjList:  adjList,
		RevAdj:   revAdj,
	}, nil
}

// RootNodes returns all nodes with in-degree 0 (ready for initial execution).
func (d *DAG) RootNodes() []*Node {
	var roots []*Node
	for nodeID, deg := range d.InDegree {
		if deg == 0 {
			roots = append(roots, d.Nodes[nodeID])
		}
	}
	return roots
}

// IsNodeRunnable evaluates if a node's incoming edge conditions are satisfied.
func (d *DAG) IsNodeRunnable(nodeID uuid.UUID) bool {
	node := d.Nodes[nodeID]
	if node == nil || node.Status != NodePending {
		return false
	}

	parents := d.RevAdj[nodeID]
	if len(parents) == 0 {
		return true
	}

	// Group parent conditions
	for _, parentID := range parents {
		parent := d.Nodes[parentID]
		if parent == nil {
			return false
		}

		// Find edge condition from parent to node
		cond := ConditionAllSuccess
		for _, e := range d.Edges {
			if e.FromNodeID == parentID && e.ToNodeID == nodeID {
				cond = e.Condition
				break
			}
		}

		switch cond {
		case ConditionAllSuccess:
			if parent.Status != NodeSucceeded {
				return false
			}
		case ConditionOnFailure:
			if parent.Status != NodeFailed {
				return false
			}
		case ConditionAlways:
			if parent.Status != NodeSucceeded && parent.Status != NodeFailed && parent.Status != NodeSkipped {
				return false
			}
		case ConditionAnySuccess:
			// evaluated separately
		}
	}

	return true
}

// IsComplete returns true if all nodes in the DAG have reached terminal statuses.
func (d *DAG) IsComplete() bool {
	for _, n := range d.Nodes {
		switch n.Status {
		case NodePending, NodeQueued, NodeRunning:
			return false
		}
	}
	return true
}

// HasFailures returns true if any node in the DAG failed without being skipped.
func (d *DAG) HasFailures() bool {
	for _, n := range d.Nodes {
		if n.Status == NodeFailed {
			return true
		}
	}
	return false
}
