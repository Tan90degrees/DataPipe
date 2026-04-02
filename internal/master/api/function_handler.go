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

type FunctionHandler struct {
	repo *repository.FunctionRepository
}

func NewFunctionHandler(db *gorm.DB) *FunctionHandler {
	return &FunctionHandler{
		repo: repository.NewFunctionRepository(db),
	}
}

type CreateFunctionRequest struct {
	Name       string                 `json:"name" binding:"required"`
	Type       models.FunctionType    `json:"type" binding:"required"`
	Version    string                 `json:"version" binding:"required"`
	Definition models.FunctionDefinition `json:"definition" binding:"required"`
	Image      string                 `json:"image"`
}

type UpdateFunctionRequest struct {
	Definition models.FunctionDefinition `json:"definition"`
	Image      string                   `json:"image"`
	Status     models.FunctionStatus    `json:"status"`
}

type ValidateFunctionRequest struct {
	InputData map[string]interface{} `json:"input_data"`
}

type FunctionListResponse struct {
	Data   []*models.Function `json:"data"`
	Total  int64              `json:"total"`
	Offset int                `json:"offset"`
	Limit  int                `json:"limit"`
}

func (h *FunctionHandler) Create(c *gin.Context) {
	var req CreateFunctionRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, errors.NewInvalidRequestf("Invalid request body"))
		return
	}

	exists, err := h.repo.ExistsByName(c.Request.Context(), req.Name)
	if err != nil {
		logging.Error(fmt.Sprintf("Failed to check function existence: %v", err))
		c.JSON(http.StatusInternalServerError, errors.NewInternalf("Failed to check function existence"))
		return
	}

	if exists {
		c.JSON(http.StatusConflict, errors.Newf(errors.ErrCodeConflict, "Function with this name already exists"))
		return
	}

	function := &models.Function{
		Name:       req.Name,
		Type:       req.Type,
		Version:    req.Version,
		Definition: req.Definition,
		Image:      req.Image,
	}

	if err := h.repo.Create(c.Request.Context(), function); err != nil {
		logging.Error(fmt.Sprintf("Failed to create function: %v", err))
		c.JSON(http.StatusInternalServerError, errors.NewInternalf("Failed to create function"))
		return
	}

	c.JSON(http.StatusCreated, function)
}

func (h *FunctionHandler) List(c *gin.Context) {
	offset, _ := strconv.Atoi(c.DefaultQuery("offset", "0"))
	limit, _ := strconv.Atoi(c.DefaultQuery("limit", "20"))
	functionType := c.Query("type")

	if limit > 100 {
		limit = 100
	}

	var functions []*models.Function
	var total int64
	var err error

	if functionType != "" {
		functions, total, err = h.repo.ListByType(c.Request.Context(), models.FunctionType(functionType), offset, limit)
	} else {
		functions, total, err = h.repo.List(c.Request.Context(), offset, limit)
	}

	if err != nil {
		logging.Error(fmt.Sprintf("Failed to list functions: %v", err))
		c.JSON(http.StatusInternalServerError, errors.NewInternalf("Failed to list functions"))
		return
	}

	c.JSON(http.StatusOK, FunctionListResponse{
		Data:   functions,
		Total:  total,
		Offset: offset,
		Limit:  limit,
	})
}

func (h *FunctionHandler) Get(c *gin.Context) {
	name := c.Param("name")

	function, err := h.repo.GetByName(c.Request.Context(), name)
	if err != nil {
		if err == gorm.ErrRecordNotFound {
			c.JSON(http.StatusNotFound, errors.NewNotFoundf("Function not found"))
			return
		}
		logging.Error(fmt.Sprintf("Failed to get function: %v", err))
		c.JSON(http.StatusInternalServerError, errors.NewInternalf("Failed to get function"))
		return
	}

	c.JSON(http.StatusOK, function)
}

func (h *FunctionHandler) Update(c *gin.Context) {
	name := c.Param("name")

	function, err := h.repo.GetByName(c.Request.Context(), name)
	if err != nil {
		if err == gorm.ErrRecordNotFound {
			c.JSON(http.StatusNotFound, errors.NewNotFoundf("Function not found"))
			return
		}
		logging.Error(fmt.Sprintf("Failed to get function: %v", err))
		c.JSON(http.StatusInternalServerError, errors.NewInternalf("Failed to get function"))
		return
	}

	var req UpdateFunctionRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, errors.NewInvalidRequestf("Invalid request body"))
		return
	}

	if req.Definition.InputType.Type != "" {
		function.Definition = req.Definition
	}
	if req.Image != "" {
		function.Image = req.Image
	}
	if req.Status != "" {
		function.Status = req.Status
	}

	if err := h.repo.Update(c.Request.Context(), function); err != nil {
		logging.Error(fmt.Sprintf("Failed to update function: %v", err))
		c.JSON(http.StatusInternalServerError, errors.NewInternalf("Failed to update function"))
		return
	}

	c.JSON(http.StatusOK, function)
}

func (h *FunctionHandler) Delete(c *gin.Context) {
	name := c.Param("name")

	function, err := h.repo.GetByName(c.Request.Context(), name)
	if err != nil {
		if err == gorm.ErrRecordNotFound {
			c.JSON(http.StatusNotFound, errors.NewNotFoundf("Function not found"))
			return
		}
		logging.Error(fmt.Sprintf("Failed to get function: %v", err))
		c.JSON(http.StatusInternalServerError, errors.NewInternalf("Failed to get function"))
		return
	}

	if err := h.repo.Delete(c.Request.Context(), function.ID); err != nil {
		logging.Error(fmt.Sprintf("Failed to delete function: %v", err))
		c.JSON(http.StatusInternalServerError, errors.NewInternalf("Failed to delete function"))
		return
	}

	c.JSON(http.StatusOK, gin.H{"message": "Function deleted successfully"})
}

func (h *FunctionHandler) Validate(c *gin.Context) {
	name := c.Param("name")

	function, err := h.repo.GetByName(c.Request.Context(), name)
	if err != nil {
		if err == gorm.ErrRecordNotFound {
			c.JSON(http.StatusNotFound, errors.NewNotFoundf("Function not found"))
			return
		}
		logging.Error(fmt.Sprintf("Failed to get function: %v", err))
		c.JSON(http.StatusInternalServerError, errors.NewInternalf("Failed to get function"))
		return
	}

	var req ValidateFunctionRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, errors.NewInvalidRequestf("Invalid request body"))
		return
	}

	validationResult := ValidateFunctionDefinition(function.Definition, req.InputData)

	c.JSON(http.StatusOK, gin.H{
		"valid":   validationResult.Valid,
		"errors":  validationResult.Errors,
		"warnings": validationResult.Warnings,
	})
}

type ValidationResult struct {
	Valid    bool     `json:"valid"`
	Errors   []string `json:"errors"`
	Warnings []string `json:"warnings"`
}

func ValidateFunctionDefinition(definition models.FunctionDefinition, inputData map[string]interface{}) ValidationResult {
	result := ValidationResult{
		Valid:    true,
		Errors:   []string{},
		Warnings: []string{},
	}

	if definition.InputType.Type == "" {
		result.Valid = false
		result.Errors = append(result.Errors, "Input type is required")
	}

	if definition.OutputType.Type == "" {
		result.Warnings = append(result.Warnings, "Output type is not specified")
	}

	return result
}
