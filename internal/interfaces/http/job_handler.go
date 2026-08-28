package http

import (
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"

	appJob "github.com/forgeflow/forgeflow/internal/application/job"
	"github.com/forgeflow/forgeflow/internal/domain/job"
	"github.com/forgeflow/forgeflow/internal/infrastructure/postgres"
	"github.com/forgeflow/forgeflow/internal/ports"
)

// SubmitJobRequest defines the JSON schema for job creation.
type SubmitJobRequest struct {
	QueueID        *string         `json:"queue_id"`
	QueueName      string          `json:"queue_name"`
	TaskType       string          `json:"task_type" binding:"required"`
	Payload        json.RawMessage `json:"payload"`
	Priority       int             `json:"priority"`
	MaxAttempts    int             `json:"max_attempts"`
	ScheduledAt    *time.Time      `json:"scheduled_at"`
	TimeoutSeconds *int            `json:"timeout_seconds"`
}

type JobHandler struct {
	jobSvc    *appJob.Service
	queueRepo ports.QueueRepository
}

func NewJobHandler(jobSvc *appJob.Service, queueRepo ports.QueueRepository) *JobHandler {
	return &JobHandler{
		jobSvc:    jobSvc,
		queueRepo: queueRepo,
	}
}

// HandleSubmitJob handles POST /api/v1/jobs with Idempotency-Key support.
func (h *JobHandler) HandleSubmitJob(c *gin.Context) {
	var req SubmitJobRequest
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

	// Resolve or fallback User ID (for unauthenticated or demo mode)
	userID := uuid.Nil
	if userVal, exists := c.Get("user_id"); exists {
		if uid, ok := userVal.(uuid.UUID); ok {
			userID = uid
		}
	}
	if userID == uuid.Nil {
		// Default system user
		userID = uuid.MustParse("00000000-0000-0000-0000-000000000001")
	}

	// Resolve Queue
	queueID := uuid.Nil
	queueName := req.QueueName
	if queueName == "" {
		queueName = "default"
	}

	if req.QueueID != nil && *req.QueueID != "" {
		if parsed, err := uuid.Parse(*req.QueueID); err == nil {
			queueID = parsed
		}
	}

	if queueID == uuid.Nil && h.queueRepo != nil {
		q, err := h.queueRepo.GetByName(c.Request.Context(), queueName)
		if err == nil && q != nil {
			queueID = q.ID
		}
	}

	if queueID == uuid.Nil {
		// Fallback default queue UUID
		queueID = uuid.MustParse("00000000-0000-0000-0000-000000000002")
	}

	scheduledAt := time.Now().UTC()
	if req.ScheduledAt != nil && !req.ScheduledAt.IsZero() {
		scheduledAt = *req.ScheduledAt
	}

	maxAttempts := req.MaxAttempts
	if maxAttempts <= 0 {
		maxAttempts = 3
	}

	var idempotencyKey *string
	idemHeader := c.GetHeader("Idempotency-Key")
	if idemHeader != "" {
		idempotencyKey = &idemHeader
	}

	cmd := appJob.SubmitJobCommand{
		UserID:         userID,
		QueueID:        queueID,
		QueueName:      queueName,
		TaskType:       req.TaskType,
		Payload:        req.Payload,
		Priority:       req.Priority,
		MaxAttempts:    maxAttempts,
		ScheduledAt:    scheduledAt,
		TimeoutSeconds: req.TimeoutSeconds,
		IdempotencyKey: idempotencyKey,
	}

	createdJob, isDuplicate, cachedResponse, err := h.jobSvc.Submit(c.Request.Context(), cmd)
	if err != nil {
		if errors.Is(err, postgres.ErrIdempotencyConflict) {
			c.JSON(http.StatusConflict, gin.H{
				"error": gin.H{
					"code":       "IDEMPOTENCY_CONFLICT",
					"message":    "An existing request with this Idempotency-Key already exists with a different payload",
					"request_id": c.Writer.Header().Get("X-Request-ID"),
				},
			})
			return
		}

		c.JSON(http.StatusInternalServerError, gin.H{
			"error": gin.H{
				"code":       "JOB_CREATION_FAILED",
				"message":    err.Error(),
				"request_id": c.Writer.Header().Get("X-Request-ID"),
			},
		})
		return
	}

	if isDuplicate && len(cachedResponse) > 0 {
		c.Header("X-Idempotency-Replay", "true")
		c.Data(http.StatusOK, "application/json", cachedResponse)
		return
	}

	c.JSON(http.StatusCreated, gin.H{
		"job":        createdJob,
		"request_id": c.Writer.Header().Get("X-Request-ID"),
	})
}

// HandleGetJob handles GET /api/v1/jobs/:id.
func (h *JobHandler) HandleGetJob(c *gin.Context) {
	idStr := c.Param("id")
	id, err := uuid.Parse(idStr)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": gin.H{
				"code":       "INVALID_JOB_ID",
				"message":    "Job ID must be a valid UUID",
				"request_id": c.Writer.Header().Get("X-Request-ID"),
			},
		})
		return
	}

	j, attempts, err := h.jobSvc.GetByID(c.Request.Context(), id)
	if err != nil {
		if errors.Is(err, job.ErrJobNotFound) {
			c.JSON(http.StatusNotFound, gin.H{
				"error": gin.H{
					"code":       "JOB_NOT_FOUND",
					"message":    "Job not found",
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
		"job":      j,
		"attempts": attempts,
	})
}

// HandleListJobs handles GET /api/v1/jobs with filters.
func (h *JobHandler) HandleListJobs(c *gin.Context) {
	filter := ports.JobFilter{
		Limit:  50,
		Offset: 0,
	}

	if statusParam := c.Query("status"); statusParam != "" {
		st := job.Status(statusParam)
		filter.Status = &st
	}
	if taskParam := c.Query("task_type"); taskParam != "" {
		filter.TaskType = &taskParam
	}
	if qIDParam := c.Query("queue_id"); qIDParam != "" {
		if qID, err := uuid.Parse(qIDParam); err == nil {
			filter.QueueID = &qID
		}
	}
	if wIDParam := c.Query("worker_id"); wIDParam != "" {
		if wID, err := uuid.Parse(wIDParam); err == nil {
			filter.WorkerID = &wID
		}
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
	if sortBy := c.Query("sort_by"); sortBy != "" {
		filter.SortBy = sortBy
	}
	if sortOrder := c.Query("sort_order"); sortOrder != "" {
		filter.SortOrder = sortOrder
	}

	jobs, total, err := h.jobSvc.List(c.Request.Context(), filter)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": gin.H{
				"code":       "LIST_JOBS_FAILED",
				"message":    err.Error(),
				"request_id": c.Writer.Header().Get("X-Request-ID"),
			},
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"data":   jobs,
		"total":  total,
		"limit":  filter.Limit,
		"offset": filter.Offset,
	})
}

// HandleCancelJob handles POST /api/v1/jobs/:id/cancel.
func (h *JobHandler) HandleCancelJob(c *gin.Context) {
	idStr := c.Param("id")
	id, err := uuid.Parse(idStr)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": gin.H{
				"code":       "INVALID_JOB_ID",
				"message":    "Job ID must be a valid UUID",
				"request_id": c.Writer.Header().Get("X-Request-ID"),
			},
		})
		return
	}

	if err := h.jobSvc.Cancel(c.Request.Context(), id); err != nil {
		if errors.Is(err, job.ErrJobNotFound) {
			c.JSON(http.StatusNotFound, gin.H{
				"error": gin.H{
					"code":       "JOB_NOT_FOUND",
					"message":    "Job not found",
					"request_id": c.Writer.Header().Get("X-Request-ID"),
				},
			})
			return
		}

		c.JSON(http.StatusBadRequest, gin.H{
			"error": gin.H{
				"code":       "JOB_CANCEL_FAILED",
				"message":    err.Error(),
				"request_id": c.Writer.Header().Get("X-Request-ID"),
			},
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"message":    "job cancellation successful",
		"job_id":     id,
		"request_id": c.Writer.Header().Get("X-Request-ID"),
	})
}
