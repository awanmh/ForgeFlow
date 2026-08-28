package http

import (
	"errors"
	"net/http"

	"github.com/gin-gonic/gin"

	appAuth "github.com/forgeflow/forgeflow/internal/application/auth"
	"github.com/forgeflow/forgeflow/internal/domain/user"
)

type RegisterRequest struct {
	Email    string      `json:"email" binding:"required,email"`
	Password string      `json:"password" binding:"required,min=8"`
	Name     string      `json:"name"`
	Roles    []user.Role `json:"roles"`
}

type LoginRequest struct {
	Email    string `json:"email" binding:"required,email"`
	Password string `json:"password" binding:"required"`
}

type AuthHandler struct {
	authSvc *appAuth.Service
}

func NewAuthHandler(authSvc *appAuth.Service) *AuthHandler {
	return &AuthHandler{authSvc: authSvc}
}

func (h *AuthHandler) HandleRegister(c *gin.Context) {
	var req RegisterRequest
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

	cmd := appAuth.RegisterCommand{
		Email:    req.Email,
		Password: req.Password,
		Name:     req.Name,
		Roles:    req.Roles,
	}

	result, err := h.authSvc.Register(c.Request.Context(), cmd)
	if err != nil {
		if errors.Is(err, user.ErrDuplicateEmail) {
			c.JSON(http.StatusConflict, gin.H{
				"error": gin.H{
					"code":       "EMAIL_EXISTS",
					"message":    "A user with this email already exists",
					"request_id": c.Writer.Header().Get("X-Request-ID"),
				},
			})
			return
		}

		c.JSON(http.StatusBadRequest, gin.H{
			"error": gin.H{
				"code":       "REGISTRATION_FAILED",
				"message":    err.Error(),
				"request_id": c.Writer.Header().Get("X-Request-ID"),
			},
		})
		return
	}

	c.JSON(http.StatusCreated, gin.H{
		"token": result.Token,
		"user":  result.User,
	})
}

func (h *AuthHandler) HandleLogin(c *gin.Context) {
	var req LoginRequest
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

	cmd := appAuth.LoginCommand{
		Email:    req.Email,
		Password: req.Password,
	}

	result, err := h.authSvc.Login(c.Request.Context(), cmd)
	if err != nil {
		c.JSON(http.StatusUnauthorized, gin.H{
			"error": gin.H{
				"code":       "INVALID_CREDENTIALS",
				"message":    "Invalid email or password",
				"request_id": c.Writer.Header().Get("X-Request-ID"),
			},
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"token": result.Token,
		"user":  result.User,
	})
}

func (h *AuthHandler) HandleMe(c *gin.Context) {
	userID := GetContextUserID(c)
	if userID.String() == "00000000-0000-0000-0000-000000000000" {
		c.JSON(http.StatusUnauthorized, gin.H{
			"error": gin.H{
				"code":       "UNAUTHORIZED",
				"message":    "Not authenticated",
				"request_id": c.Writer.Header().Get("X-Request-ID"),
			},
		})
		return
	}

	u, err := h.authSvc.GetProfile(c.Request.Context(), userID)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{
			"error": gin.H{
				"code":       "USER_NOT_FOUND",
				"message":    "User not found",
				"request_id": c.Writer.Header().Get("X-Request-ID"),
			},
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"user": u,
	})
}
