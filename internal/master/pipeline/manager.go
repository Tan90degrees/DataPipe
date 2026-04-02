package pipeline

import (
	"context"
	"fmt"

	"gorm.io/gorm"

	"datapipe/internal/common/errors"
	"datapipe/internal/common/logging"
	"datapipe/internal/models"
	"datapipe/internal/repository"
)

type Manager struct {
	db        *gorm.DB
	repo      *repository.PipelineRepository
	validator *Validator
	logger    logging.Logger
}

func NewManager(db *gorm.DB) *Manager {
	return &Manager{
		db:        db,
		repo:      repository.NewPipelineRepository(db),
		validator: NewValidator(),
		logger:    logging.New("pipeline-manager"),
	}
}

func (m *Manager) Create(ctx context.Context, pipeline *models.Pipeline) error {
	if err := m.validator.ValidateDefinition(&pipeline.Definition); err != nil {
		return errors.NewValidationFailed(err.Error())
	}

	if err := m.repo.Create(ctx, pipeline); err != nil {
		return fmt.Errorf("failed to create pipeline: %w", err)
	}

	return nil
}

func (m *Manager) GetByID(ctx context.Context, id string) (*models.Pipeline, error) {
	pipeline, err := m.repo.GetByID(ctx, id)
	if err != nil {
		if err == gorm.ErrRecordNotFound {
			return nil, errors.NewPipelineNotFound(id)
		}
		return nil, err
	}
	return pipeline, nil
}

func (m *Manager) GetByName(ctx context.Context, name string) (*models.Pipeline, error) {
	pipeline, err := m.repo.GetByName(ctx, name)
	if err != nil {
		if err == gorm.ErrRecordNotFound {
			return nil, errors.NewNotFoundf("pipeline not found: %s", name)
		}
		return nil, err
	}
	return pipeline, nil
}

func (m *Manager) List(ctx context.Context, offset, limit int) ([]*models.Pipeline, int64, error) {
	return m.repo.List(ctx, offset, limit)
}

func (m *Manager) Update(ctx context.Context, pipeline *models.Pipeline, changelog string) error {
	if err := m.validator.ValidateDefinition(&pipeline.Definition); err != nil {
		return errors.NewValidationFailed(err.Error())
	}

	_, err := m.repo.CreateNewVersion(ctx, pipeline, changelog)
	if err != nil {
		return fmt.Errorf("failed to update pipeline: %w", err)
	}

	return nil
}

func (m *Manager) Delete(ctx context.Context, id string) error {
	if err := m.repo.Delete(ctx, id); err != nil {
		return fmt.Errorf("failed to delete pipeline: %w", err)
	}
	return nil
}

func (m *Manager) Start(ctx context.Context, id string) error {
	pipeline, err := m.repo.GetByID(ctx, id)
	if err != nil {
		return err
	}

	if pipeline.Status != models.PipelineStatusDraft && pipeline.Status != models.PipelineStatusStopped {
		return errors.NewInvalidRequestf("Pipeline cannot be started in current state")
	}

	if err := m.repo.UpdateStatus(ctx, id, models.PipelineStatusActive); err != nil {
		return err
	}

	m.logger.Info(fmt.Sprintf("Pipeline started: %s", id))
	return nil
}

func (m *Manager) Stop(ctx context.Context, id string) error {
	pipeline, err := m.repo.GetByID(ctx, id)
	if err != nil {
		return err
	}

	if pipeline.Status != models.PipelineStatusActive && pipeline.Status != models.PipelineStatusPaused {
		return errors.NewInvalidRequestf("Pipeline cannot be stopped in current state")
	}

	if err := m.repo.UpdateStatus(ctx, id, models.PipelineStatusStopped); err != nil {
		return err
	}

	m.logger.Info(fmt.Sprintf("Pipeline stopped: %s", id))
	return nil
}

func (m *Manager) Pause(ctx context.Context, id string) error {
	pipeline, err := m.repo.GetByID(ctx, id)
	if err != nil {
		return err
	}

	if pipeline.Status != models.PipelineStatusActive {
		return errors.NewInvalidRequestf("Pipeline can only be paused when active")
	}

	if err := m.repo.UpdateStatus(ctx, id, models.PipelineStatusPaused); err != nil {
		return err
	}

	m.logger.Info(fmt.Sprintf("Pipeline paused: %s", id))
	return nil
}

func (m *Manager) Resume(ctx context.Context, id string) error {
	pipeline, err := m.repo.GetByID(ctx, id)
	if err != nil {
		return err
	}

	if pipeline.Status != models.PipelineStatusPaused {
		return errors.NewInvalidRequestf("Pipeline can only be resumed when paused")
	}

	if err := m.repo.UpdateStatus(ctx, id, models.PipelineStatusActive); err != nil {
		return err
	}

	m.logger.Info(fmt.Sprintf("Pipeline resumed: %s", id))
	return nil
}

func (m *Manager) GetVersions(ctx context.Context, pipelineID string) ([]*models.PipelineVersion, error) {
	return m.repo.GetVersions(ctx, pipelineID)
}

func (m *Manager) GetVersion(ctx context.Context, pipelineID string, version int) (*models.PipelineVersion, error) {
	return m.repo.GetVersion(ctx, pipelineID, version)
}

func (m *Manager) RollbackToVersion(ctx context.Context, pipelineID string, version int) error {
	if err := m.repo.RollbackToVersion(ctx, pipelineID, version); err != nil {
		return fmt.Errorf("failed to rollback pipeline: %w", err)
	}
	return nil
}

func (m *Manager) Validate(ctx context.Context, definition *models.Definition) error {
	return m.validator.ValidateDefinition(definition)
}
