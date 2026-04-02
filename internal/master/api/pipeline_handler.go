package api

import (
	"fmt"
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"

	"datapipe/internal/common/errors"
	"datapipe/internal/common/logging"
	"datapipe/internal/models"
	"datapipe/internal/repository"
)

type PipelineHandler struct {
	repo *repository.PipelineRepository
}

func NewPipelineHandler(db *gorm.DB) *PipelineHandler {
	return &PipelineHandler{
		repo: repository.NewPipelineRepository(db),
	}
}

type CreatePipelineRequest struct {
	Name        string                 `json:"name" binding:"required"`
	Description string                 `json:"description"`
	Definition  models.Definition       `json:"definition" binding:"required"`
	CreatedBy   string                 `json:"created_by"`
}

type UpdatePipelineRequest struct {
	Name        string           `json:"name"`
	Description string           `json:"description"`
	Definition  models.Definition `json:"definition"`
	Changelog   string           `json:"changelog"`
}

type ListResponse struct {
	Data []*models.Pipeline `json:"data"`
	Total int64             `json:"total"`
	Offset int              `json:"offset"`
	Limit int               `json:"limit"`
}

func (h *PipelineHandler) Create(c *gin.Context) {
	var req CreatePipelineRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, errors.NewInvalidRequestf("Invalid request body"))
		return
	}

	pipeline := &models.Pipeline{
		Name:        req.Name,
		Description: req.Description,
		Definition:  req.Definition,
		CreatedBy:   req.CreatedBy,
	}

	if err := h.repo.Create(c.Request.Context(), pipeline); err != nil {
		logging.Error(fmt.Sprintf("Failed to create pipeline: %v", err))
		c.JSON(http.StatusInternalServerError, errors.NewInternalf("Failed to create pipeline"))
		return
	}

	c.JSON(http.StatusCreated, pipeline)
}

func (h *PipelineHandler) List(c *gin.Context) {
	offset, _ := strconv.Atoi(c.DefaultQuery("offset", "0"))
	limit, _ := strconv.Atoi(c.DefaultQuery("limit", "20"))

	if limit > 100 {
		limit = 100
	}

	pipelines, total, err := h.repo.List(c.Request.Context(), offset, limit)
	if err != nil {
		logging.Error(fmt.Sprintf("Failed to list pipelines: %v", err))
		c.JSON(http.StatusInternalServerError, errors.NewInternalf("Failed to list pipelines"))
		return
	}

	c.JSON(http.StatusOK, ListResponse{
		Data:   pipelines,
		Total:  total,
		Offset: offset,
		Limit:  limit,
	})
}

func (h *PipelineHandler) Get(c *gin.Context) {
	id := c.Param("id")

	pipeline, err := h.repo.GetByID(c.Request.Context(), id)
	if err != nil {
		if err == gorm.ErrRecordNotFound {
			c.JSON(http.StatusNotFound, errors.NewNotFoundf("Pipeline not found"))
			return
		}
		logging.Error(fmt.Sprintf("Failed to get pipeline: %v", err))
		c.JSON(http.StatusInternalServerError, errors.NewInternalf("Failed to get pipeline"))
		return
	}

	c.JSON(http.StatusOK, pipeline)
}

func (h *PipelineHandler) Update(c *gin.Context) {
	id := c.Param("id")

	pipeline, err := h.repo.GetByID(c.Request.Context(), id)
	if err != nil {
		if err == gorm.ErrRecordNotFound {
			c.JSON(http.StatusNotFound, errors.NewNotFoundf("Pipeline not found"))
			return
		}
		logging.Error(fmt.Sprintf("Failed to get pipeline: %v", err))
		c.JSON(http.StatusInternalServerError, errors.NewInternalf("Failed to get pipeline"))
		return
	}

	var req UpdatePipelineRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, errors.NewInvalidRequestf("Invalid request body"))
		return
	}

	if req.Name != "" {
		pipeline.Name = req.Name
	}
	if req.Description != "" {
		pipeline.Description = req.Description
	}
	if req.Definition.Nodes != nil {
		pipeline.Definition = req.Definition
	}

	_, err = h.repo.CreateNewVersion(c.Request.Context(), pipeline, req.Changelog)
	if err != nil {
		logging.Error(fmt.Sprintf("Failed to update pipeline: %v", err))
		c.JSON(http.StatusInternalServerError, errors.NewInternalf("Failed to update pipeline"))
		return
	}

	c.JSON(http.StatusOK, pipeline)
}

func (h *PipelineHandler) Delete(c *gin.Context) {
	id := c.Param("id")

	_, err := h.repo.GetByID(c.Request.Context(), id)
	if err != nil {
		if err == gorm.ErrRecordNotFound {
			c.JSON(http.StatusNotFound, errors.NewNotFoundf("Pipeline not found"))
			return
		}
		logging.Error(fmt.Sprintf("Failed to get pipeline: %v", err))
		c.JSON(http.StatusInternalServerError, errors.NewInternalf("Failed to get pipeline"))
		return
	}

	if err := h.repo.Delete(c.Request.Context(), id); err != nil {
		logging.Error(fmt.Sprintf("Failed to delete pipeline: %v", err))
		c.JSON(http.StatusInternalServerError, errors.NewInternalf("Failed to delete pipeline"))
		return
	}

	c.JSON(http.StatusOK, gin.H{"message": "Pipeline deleted successfully"})
}

func (h *PipelineHandler) Start(c *gin.Context) {
	id := c.Param("id")

	pipeline, err := h.repo.GetByID(c.Request.Context(), id)
	if err != nil {
		if err == gorm.ErrRecordNotFound {
			c.JSON(http.StatusNotFound, errors.NewNotFoundf("Pipeline not found"))
			return
		}
		logging.Error(fmt.Sprintf("Failed to get pipeline: %v", err))
		c.JSON(http.StatusInternalServerError, errors.NewInternalf("Failed to get pipeline"))
		return
	}

	if pipeline.Status != models.PipelineStatusDraft && pipeline.Status != models.PipelineStatusStopped {
		c.JSON(http.StatusBadRequest, errors.NewInvalidRequestf("Pipeline cannot be started in current state"))
		return
	}

	if err := h.repo.UpdateStatus(c.Request.Context(), id, models.PipelineStatusActive); err != nil {
		logging.Error(fmt.Sprintf("Failed to start pipeline: %v", err))
		c.JSON(http.StatusInternalServerError, errors.NewInternalf("Failed to start pipeline"))
		return
	}

	c.JSON(http.StatusOK, gin.H{"message": "Pipeline started successfully", "status": models.PipelineStatusActive})
}

func (h *PipelineHandler) Stop(c *gin.Context) {
	id := c.Param("id")

	pipeline, err := h.repo.GetByID(c.Request.Context(), id)
	if err != nil {
		if err == gorm.ErrRecordNotFound {
			c.JSON(http.StatusNotFound, errors.NewNotFoundf("Pipeline not found"))
			return
		}
		logging.Error(fmt.Sprintf("Failed to get pipeline: %v", err))
		c.JSON(http.StatusInternalServerError, errors.NewInternalf("Failed to get pipeline"))
		return
	}

	if pipeline.Status != models.PipelineStatusActive && pipeline.Status != models.PipelineStatusPaused {
		c.JSON(http.StatusBadRequest, errors.NewInvalidRequestf("Pipeline cannot be stopped in current state"))
		return
	}

	if err := h.repo.UpdateStatus(c.Request.Context(), id, models.PipelineStatusStopped); err != nil {
		logging.Error(fmt.Sprintf("Failed to stop pipeline: %v", err))
		c.JSON(http.StatusInternalServerError, errors.NewInternalf("Failed to stop pipeline"))
		return
	}

	c.JSON(http.StatusOK, gin.H{"message": "Pipeline stopped successfully", "status": models.PipelineStatusStopped})
}

func (h *PipelineHandler) Pause(c *gin.Context) {
	id := c.Param("id")

	pipeline, err := h.repo.GetByID(c.Request.Context(), id)
	if err != nil {
		if err == gorm.ErrRecordNotFound {
			c.JSON(http.StatusNotFound, errors.NewNotFoundf("Pipeline not found"))
			return
		}
		logging.Error(fmt.Sprintf("Failed to get pipeline: %v", err))
		c.JSON(http.StatusInternalServerError, errors.NewInternalf("Failed to get pipeline"))
		return
	}

	if pipeline.Status != models.PipelineStatusActive {
		c.JSON(http.StatusBadRequest, errors.NewInvalidRequestf("Pipeline can only be paused when active"))
		return
	}

	if err := h.repo.UpdateStatus(c.Request.Context(), id, models.PipelineStatusPaused); err != nil {
		logging.Error(fmt.Sprintf("Failed to pause pipeline: %v", err))
		c.JSON(http.StatusInternalServerError, errors.NewInternalf("Failed to pause pipeline"))
		return
	}

	c.JSON(http.StatusOK, gin.H{"message": "Pipeline paused successfully", "status": models.PipelineStatusPaused})
}

func (h *PipelineHandler) Resume(c *gin.Context) {
	id := c.Param("id")

	pipeline, err := h.repo.GetByID(c.Request.Context(), id)
	if err != nil {
		if err == gorm.ErrRecordNotFound {
			c.JSON(http.StatusNotFound, errors.NewNotFoundf("Pipeline not found"))
			return
		}
		logging.Error(fmt.Sprintf("Failed to get pipeline: %v", err))
		c.JSON(http.StatusInternalServerError, errors.NewInternalf("Failed to get pipeline"))
		return
	}

	if pipeline.Status != models.PipelineStatusPaused {
		c.JSON(http.StatusBadRequest, errors.NewInvalidRequestf("Pipeline can only be resumed when paused"))
		return
	}

	if err := h.repo.UpdateStatus(c.Request.Context(), id, models.PipelineStatusActive); err != nil {
		logging.Error(fmt.Sprintf("Failed to resume pipeline: %v", err))
		c.JSON(http.StatusInternalServerError, errors.NewInternalf("Failed to resume pipeline"))
		return
	}

	c.JSON(http.StatusOK, gin.H{"message": "Pipeline resumed successfully", "status": models.PipelineStatusActive})
}

func (h *PipelineHandler) Versions(c *gin.Context) {
	id := c.Param("id")

	_, err := h.repo.GetByID(c.Request.Context(), id)
	if err != nil {
		if err == gorm.ErrRecordNotFound {
			c.JSON(http.StatusNotFound, errors.NewNotFoundf("Pipeline not found"))
			return
		}
		logging.Error(fmt.Sprintf("Failed to get pipeline: %v", err))
		c.JSON(http.StatusInternalServerError, errors.NewInternalf("Failed to get pipeline"))
		return
	}

	versions, err := h.repo.GetVersions(c.Request.Context(), id)
	if err != nil {
		logging.Error(fmt.Sprintf("Failed to get pipeline versions: %v", err))
		c.JSON(http.StatusInternalServerError, errors.NewInternalf("Failed to get pipeline versions"))
		return
	}

	c.JSON(http.StatusOK, versions)
}
