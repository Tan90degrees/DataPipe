package repository

import (
	"context"

	"github.com/google/uuid"
	"gorm.io/gorm"

	"datapipe/internal/models"
)

type FunctionRepository struct {
	db *gorm.DB
}

func NewFunctionRepository(db *gorm.DB) *FunctionRepository {
	return &FunctionRepository{db: db}
}

func (r *FunctionRepository) Create(ctx context.Context, function *models.Function) error {
	if function.ID == "" {
		function.ID = uuid.New().String()
	}
	if function.Status == "" {
		function.Status = models.FunctionStatusRegistered
	}
	return r.db.WithContext(ctx).Create(function).Error
}

func (r *FunctionRepository) GetByID(ctx context.Context, id string) (*models.Function, error) {
	var function models.Function
	err := r.db.WithContext(ctx).First(&function, "id = ?", id).Error
	if err != nil {
		return nil, err
	}
	return &function, nil
}

func (r *FunctionRepository) GetByName(ctx context.Context, name string) (*models.Function, error) {
	var function models.Function
	err := r.db.WithContext(ctx).Where("name = ?", name).First(&function).Error
	if err != nil {
		return nil, err
	}
	return &function, nil
}

func (r *FunctionRepository) GetByNameAndVersion(ctx context.Context, name, version string) (*models.Function, error) {
	var function models.Function
	err := r.db.WithContext(ctx).Where("name = ? AND version = ?", name, version).First(&function).Error
	if err != nil {
		return nil, err
	}
	return &function, nil
}

func (r *FunctionRepository) List(ctx context.Context, offset, limit int) ([]*models.Function, int64, error) {
	var functions []*models.Function
	var total int64

	query := r.db.WithContext(ctx).Model(&models.Function{})

	if err := query.Count(&total).Error; err != nil {
		return nil, 0, err
	}

	if offset >= 0 && limit > 0 {
		query = query.Offset(offset).Limit(limit)
	}

	if err := query.Order("created_at DESC").Find(&functions).Error; err != nil {
		return nil, 0, err
	}

	return functions, total, nil
}

func (r *FunctionRepository) ListByType(ctx context.Context, functionType models.FunctionType, offset, limit int) ([]*models.Function, int64, error) {
	var functions []*models.Function
	var total int64

	query := r.db.WithContext(ctx).Model(&models.Function{}).Where("type = ?", functionType)

	if err := query.Count(&total).Error; err != nil {
		return nil, 0, err
	}

	if offset >= 0 && limit > 0 {
		query = query.Offset(offset).Limit(limit)
	}

	if err := query.Order("created_at DESC").Find(&functions).Error; err != nil {
		return nil, 0, err
	}

	return functions, total, nil
}

func (r *FunctionRepository) Update(ctx context.Context, function *models.Function) error {
	return r.db.WithContext(ctx).Save(function).Error
}

func (r *FunctionRepository) Delete(ctx context.Context, id string) error {
	return r.db.WithContext(ctx).Delete(&models.Function{}, "id = ?", id).Error
}

func (r *FunctionRepository) UpdateStatus(ctx context.Context, id string, status models.FunctionStatus) error {
	return r.db.WithContext(ctx).Model(&models.Function{}).Where("id = ?", id).Update("status", status).Error
}

func (r *FunctionRepository) UpdateImage(ctx context.Context, id string, image string) error {
	return r.db.WithContext(ctx).Model(&models.Function{}).Where("id = ?", id).Update("image", image).Error
}

func (r *FunctionRepository) ExistsByName(ctx context.Context, name string) (bool, error) {
	var count int64
	err := r.db.WithContext(ctx).Model(&models.Function{}).Where("name = ?", name).Count(&count).Error
	if err != nil {
		return false, err
	}
	return count > 0, nil
}

func (r *FunctionRepository) ListActive(ctx context.Context) ([]*models.Function, error) {
	var functions []*models.Function
	err := r.db.WithContext(ctx).
		Where("status = ?", models.FunctionStatusActive).
		Order("name ASC").
		Find(&functions).Error
	if err != nil {
		return nil, err
	}
	return functions, nil
}
