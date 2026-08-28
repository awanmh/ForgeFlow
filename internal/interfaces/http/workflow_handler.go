package http

import (
	"errors"
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"

	appWorkflow "github.com/forgeflow/forgeflow/internal/application/workflow"
	"github.com/forgeflow/forgeflow/internal/domain/workflow"
	"github.com/forgeflow/forgeflow/internal/ports"
)

type CreateWorkflowRequest struct {
	Name        string                      `json:"name" binding:"required"`
	Description *string                     `json:"description"`
	Nodes       []appWorkflow.CreateNodeDTO `json:"nodes" binding:"required"`
	Edges       []appWorkflow.CreateEdgeDTO `json:"edges"`
}

type WorkflowHandler struct {
	wfSvc *appWorkflow.Service
}

func NewWorkflowHandler(wfSvc *appWorkflow.Service) *WorkflowHandler {
	return &WorkflowHandler{wfSvc: wfSvc}
}

func (h *WorkflowHandler) HandleCreateWorkflow(c *gin.Context) {
	var req CreateWorkflowRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": gin.H{
				"code":       "INVALID_REQUEST",
				"message":    err.Error(),
				"request_id": c.Writer.Header().Get("X-Request-ID"),
			},
		})
		return
	}

	userID := uuid.Nil
	if userVal, exists := c.Get("user_id"); exists {
		if uid, ok := userVal.(uuid.UUID); ok {
			userID = uid
		}
	}
	if userID == uuid.Nil {
		userID = uuid.MustParse("00000000-0000-0000-0000-000000000001")
	}

	cmd := appWorkflow.CreateWorkflowCommand{
		UserID:      userID,
		Name:        req.Name,
		Description: req.Description,
		Nodes:       req.Nodes,
		Edges:       req.Edges,
	}

	wf, nodes, edges, err := h.wfSvc.Create(c.Request.Context(), cmd)
	if err != nil {
		if errors.Is(err, workflow.ErrCycleDetected) || errors.Is(err, workflow.ErrSelfReferentialEdge) || errors.Is(err, workflow.ErrNodeNotFound) {
			c.JSON(http.StatusBadRequest, gin.H{
				"error": gin.H{
					"code":       "INVALID_DAG_STRUCTURE",
					"message":    err.Error(),
					"request_id": c.Writer.Header().Get("X-Request-ID"),
				},
			})
			return
		}

		c.JSON(http.StatusInternalServerError, gin.H{
			"error": gin.H{
				"code":       "WORKFLOW_CREATION_FAILED",
				"message":    err.Error(),
				"request_id": c.Writer.Header().Get("X-Request-ID"),
			},
		})
		return
	}

	c.JSON(http.StatusCreated, gin.H{
		"workflow":   wf,
		"nodes":      nodes,
		"edges":      edges,
		"request_id": c.Writer.Header().Get("X-Request-ID"),
	})
}

func (h *WorkflowHandler) HandleGetWorkflow(c *gin.Context) {
	idStr := c.Param("id")
	id, err := uuid.Parse(idStr)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": gin.H{
				"code":       "INVALID_WORKFLOW_ID",
				"message":    "Workflow ID must be a valid UUID",
				"request_id": c.Writer.Header().Get("X-Request-ID"),
			},
		})
		return
	}

	wf, nodes, edges, err := h.wfSvc.GetByID(c.Request.Context(), id)
	if err != nil {
		if errors.Is(err, workflow.ErrWorkflowNotFound) {
			c.JSON(http.StatusNotFound, gin.H{
				"error": gin.H{
					"code":       "WORKFLOW_NOT_FOUND",
					"message":    "Workflow not found",
					"request_id": c.Writer.Header().Get("X-Request-ID"),
				},
			})
			return
		}

		c.JSON(http.StatusInternalServerError, gin.H{
			"error": gin.H{
				"code":       "INTERNAL_ERROR",
				"message":    err.Error(),
				"request_id": c.Writer.Header().Get("X-Request-ID"),
			},
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"workflow": wf,
		"nodes":    nodes,
		"edges":    edges,
	})
}

func (h *WorkflowHandler) HandleListWorkflows(c *gin.Context) {
	filter := ports.WorkflowFilter{
		Limit:  50,
		Offset: 0,
	}

	if statusParam := c.Query("status"); statusParam != "" {
		st := workflow.Status(statusParam)
		filter.Status = &st
	}
	if limitParam := c.Query("limit"); limitParam != "" {
		if l, err := strconv.Atoi(limitParam); err == nil && l > 0 && l <= 100 {
			filter.Limit = l
		}
	}
	if offsetParam := c.Query("offset"); offsetParam != "" {
		if off, err := strconv.Atoi(offsetParam); err == nil && off >= 0 {
			filter.Offset = off
		}
	}

	workflows, total, err := h.wfSvc.List(c.Request.Context(), filter)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": gin.H{
				"code":       "LIST_WORKFLOWS_FAILED",
				"message":    err.Error(),
				"request_id": c.Writer.Header().Get("X-Request-ID"),
			},
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"data":   workflows,
		"total":  total,
		"limit":  filter.Limit,
		"offset": filter.Offset,
	})
}

func (h *WorkflowHandler) HandleStartWorkflow(c *gin.Context) {
	idStr := c.Param("id")
	id, err := uuid.Parse(idStr)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": gin.H{
				"code":       "INVALID_WORKFLOW_ID",
				"message":    "Workflow ID must be a valid UUID",
				"request_id": c.Writer.Header().Get("X-Request-ID"),
			},
		})
		return
	}

	if err := h.wfSvc.Start(c.Request.Context(), id); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": gin.H{
				"code":       "START_WORKFLOW_FAILED",
				"message":    err.Error(),
				"request_id": c.Writer.Header().Get("X-Request-ID"),
			},
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"message":     "workflow execution initiated",
		"workflow_id": id,
		"request_id":  c.Writer.Header().Get("X-Request-ID"),
	})
}

func (h *WorkflowHandler) HandleCancelWorkflow(c *gin.Context) {
	idStr := c.Param("id")
	id, err := uuid.Parse(idStr)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": gin.H{
				"code":       "INVALID_WORKFLOW_ID",
				"message":    "Workflow ID must be a valid UUID",
				"request_id": c.Writer.Header().Get("X-Request-ID"),
			},
		})
		return
	}

	if err := h.wfSvc.Cancel(c.Request.Context(), id); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": gin.H{
				"code":       "CANCEL_WORKFLOW_FAILED",
				"message":    err.Error(),
				"request_id": c.Writer.Header().Get("X-Request-ID"),
			},
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"message":     "workflow cancellation successful",
		"workflow_id": id,
		"request_id":  c.Writer.Header().Get("X-Request-ID"),
	})
}
