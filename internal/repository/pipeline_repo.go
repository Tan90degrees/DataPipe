package repository

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	"gorm.io/gorm"

	"datapipe/internal/models"
)

type PipelineRepository struct {
	db *gorm.DB
}

func NewPipelineRepository(db *gorm.DB) *PipelineRepository {
	return &PipelineRepository{db: db}
}

func (r *PipelineRepository) Create(ctx context.Context, pipeline *models.Pipeline) error {
	if pipeline.ID == "" {
		pipeline.ID = uuid.New().String()
	}
	if pipeline.Version == 0 {
		pipeline.Version = 1
	}
	if pipeline.Status == "" {
		pipeline.Status = models.PipelineStatusDraft
	}
	return r.db.WithContext(ctx).Create(pipeline).Error
}

func (r *PipelineRepository) GetByID(ctx context.Context, id string) (*models.Pipeline, error) {
	var pipeline models.Pipeline
	err := r.db.WithContext(ctx).First(&pipeline, "id = ?", id).Error
	if err != nil {
		return nil, err
	}
	return &pipeline, nil
}

func (r *PipelineRepository) GetByName(ctx context.Context, name string) (*models.Pipeline, error) {
	var pipeline models.Pipeline
	err := r.db.WithContext(ctx).Where("name = ?", name).First(&pipeline).Error
	if err != nil {
		return nil, err
	}
	return &pipeline, nil
}

func (r *PipelineRepository) List(ctx context.Context, offset, limit int) ([]*models.Pipeline, int64, error) {
	var pipelines []*models.Pipeline
	var total int64

	query := r.db.WithContext(ctx).Model(&models.Pipeline{})

	if err := query.Count(&total).Error; err != nil {
		return nil, 0, err
	}

	if offset >= 0 && limit > 0 {
		query = query.Offset(offset).Limit(limit)
	}

	if err := query.Order("created_at DESC").Find(&pipelines).Error; err != nil {
		return nil, 0, err
	}

	return pipelines, total, nil
}

func (r *PipelineRepository) Update(ctx context.Context, pipeline *models.Pipeline) error {
	return r.db.WithContext(ctx).Save(pipeline).Error
}

func (r *PipelineRepository) Delete(ctx context.Context, id string) error {
	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := tx.Where("pipeline_id = ?", id).Delete(&models.PipelineVersion{}).Error; err != nil {
			return err
		}
		return tx.Delete(&models.Pipeline{}, "id = ?", id).Error
	})
}

func (r *PipelineRepository) UpdateStatus(ctx context.Context, id string, status models.PipelineStatus) error {
	return r.db.WithContext(ctx).Model(&models.Pipeline{}).Where("id = ?", id).Update("status", status).Error
}

func (r *PipelineRepository) CreateVersion(ctx context.Context, version *models.PipelineVersion) error {
	if version.ID == "" {
		version.ID = uuid.New().String()
	}
	return r.db.WithContext(ctx).Create(version).Error
}

func (r *PipelineRepository) GetVersions(ctx context.Context, pipelineID string) ([]*models.PipelineVersion, error) {
	var versions []*models.PipelineVersion
	err := r.db.WithContext(ctx).
		Where("pipeline_id = ?", pipelineID).
		Order("version DESC").
		Find(&versions).Error
	if err != nil {
		return nil, err
	}
	return versions, nil
}

func (r *PipelineRepository) GetVersion(ctx context.Context, pipelineID string, version int) (*models.PipelineVersion, error) {
	var pv models.PipelineVersion
	err := r.db.WithContext(ctx).
		Where("pipeline_id = ? AND version = ?", pipelineID, version).
		First(&pv).Error
	if err != nil {
		return nil, err
	}
	return &pv, nil
}

func (r *PipelineRepository) GetLatestVersion(ctx context.Context, pipelineID string) (*models.PipelineVersion, error) {
	var pv models.PipelineVersion
	err := r.db.WithContext(ctx).
		Where("pipeline_id = ?", pipelineID).
		Order("version DESC").
		First(&pv).Error
	if err != nil {
		return nil, err
	}
	return &pv, nil
}

func (r *PipelineRepository) CreateNewVersion(ctx context.Context, pipeline *models.Pipeline, changelog string) (*models.PipelineVersion, error) {
	var newVersion int
	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var latestVersion models.PipelineVersion
		err := tx.Where("pipeline_id = ?", pipeline.ID).Order("version DESC").First(&latestVersion).Error
		if err != nil && err != gorm.ErrRecordNotFound {
			return err
		}

		if err == gorm.ErrRecordNotFound {
			newVersion = 1
		} else {
			newVersion = latestVersion.Version + 1
		}

		pipeline.Version = newVersion
		if err := tx.Save(pipeline).Error; err != nil {
			return err
		}

		pv := &models.PipelineVersion{
			ID:         uuid.New().String(),
			PipelineID: pipeline.ID,
			Version:    newVersion,
			Definition: pipeline.Definition,
			Changelog:  changelog,
			CreatedBy:  pipeline.CreatedBy,
		}

		return tx.Create(pv).Error
	})

	if err != nil {
		return nil, err
	}

	return r.GetVersion(ctx, pipeline.ID, newVersion)
}

func (r *PipelineRepository) RollbackToVersion(ctx context.Context, pipelineID string, targetVersion int) error {
	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		pv, err := r.getVersionTx(tx, pipelineID, targetVersion)
		if err != nil {
			return fmt.Errorf("version not found: %w", err)
		}

		result := tx.Model(&models.Pipeline{}).Where("id = ?", pipelineID).Updates(map[string]interface{}{
			"definition": pv.Definition,
			"version":    targetVersion,
		})
		if result.Error != nil {
			return result.Error
		}

		if result.RowsAffected == 0 {
			return gorm.ErrRecordNotFound
		}

		return nil
	})
}

func (r *PipelineRepository) getVersionTx(tx *gorm.DB, pipelineID string, version int) (*models.PipelineVersion, error) {
	var pv models.PipelineVersion
	err := tx.Where("pipeline_id = ? AND version = ?", pipelineID, version).First(&pv).Error
	if err != nil {
		return nil, err
	}
	return &pv, nil
}
