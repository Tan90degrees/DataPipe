package validation

import (
	"context"
	"crypto/md5"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"hash"
	"io"
	"math"
	"os"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"datapipe/internal/common/errors"
	"datapipe/internal/models"
)

type ValidationType string

const (
	ValidationTypeSchema   ValidationType = "schema"
	ValidationTypeChecksum ValidationType = "checksum"
	ValidationTypeQuality  ValidationType = "quality"
	ValidationTypeCustom   ValidationType = "custom"
)

type ValidationResult struct {
	Valid      bool
	Errors     []*ValidationError
	Warnings   []*ValidationWarning
	Metadata   map[string]interface{}
	Duration   time.Duration
	Timestamp  time.Time
}

func NewValidationResult() *ValidationResult {
	return &ValidationResult{
		Errors:    make([]*ValidationError, 0),
		Warnings: make([]*ValidationWarning, 0),
		Metadata: make(map[string]interface{}),
		Timestamp: time.Now(),
	}
}

func (r *ValidationResult) AddError(field, message string) {
	r.Errors = append(r.Errors, &ValidationError{
		Field:   field,
		Message: message,
	})
	r.Valid = false
}

func (r *ValidationResult) AddWarning(field, message string) {
	r.Warnings = append(r.Warnings, &ValidationWarning{
		Field:   field,
		Message: message,
	})
}

func (r *ValidationResult) Merge(other *ValidationResult) {
	r.Errors = append(r.Errors, other.Errors...)
	r.Warnings = append(r.Warnings, other.Warnings...)
	r.Valid = r.Valid && other.Valid
	for k, v := range other.Metadata {
		r.Metadata[k] = v
	}
	r.Duration += other.Duration
}

type ValidationError struct {
	Field   string `json:"field"`
	Message string `json:"message"`
	Code    string `json:"code"`
}

func (e *ValidationError) Error() string {
	return fmt.Sprintf("[%s] %s: %s", e.Code, e.Field, e.Message)
}

type ValidationWarning struct {
	Field   string `json:"field"`
	Message string `json:"message"`
	Code    string `json:"code"`
}

func (w *ValidationWarning) Error() string {
	return fmt.Sprintf("[%s] %s: %s", w.Code, w.Field, w.Message)
}

type Validator interface {
	Validate(ctx context.Context, data interface{}) (*ValidationResult, error)
	Type() ValidationType
	Name() string
}

type SchemaValidator struct {
	mu     sync.RWMutex
	schema *models.Schema
	rules  map[string]*FieldRules
}

type FieldRules struct {
	Required    bool
	Type        string
	MinLength   *int
	MaxLength   *int
	MinValue    *float64
	MaxValue    *float64
	Pattern     *regexp.Regexp
	Enum        []interface{}
	CustomFunc  func(interface{}) error
}

func NewSchemaValidator(schema *models.Schema) *SchemaValidator {
	return &SchemaValidator{
		schema: schema,
		rules:  make(map[string]*FieldRules),
	}
}

func (v *SchemaValidator) AddRules(fieldName string, rules *FieldRules) {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.rules[fieldName] = rules
}

func (v *SchemaValidator) Validate(ctx context.Context, data interface{}) (*ValidationResult, error) {
	v.mu.RLock()
	defer v.mu.RUnlock()

	result := NewValidationResult()
	start := time.Now()

	if data == nil {
		result.AddError("data", "data is nil")
		result.Duration = time.Since(start)
		return result, nil
	}

	dataMap, ok := data.(map[string]interface{})
	if !ok {
		jsonData, err := json.Marshal(data)
		if err != nil {
			result.AddError("data", "failed to marshal data")
			result.Duration = time.Since(start)
			return result, nil
		}
		if err := json.Unmarshal(jsonData, &dataMap); err != nil {
			result.AddError("data", "failed to unmarshal data to map")
			result.Duration = time.Since(start)
			return result, nil
		}
	}

	if v.schema != nil {
		for _, field := range v.schema.Fields {
			value, exists := dataMap[field.Name]
			rules := v.rules[field.Name]

			if rules != nil && rules.Required && !exists {
				result.AddError(field.Name, "required field is missing")
				continue
			}

			if !exists {
				continue
			}

			if rules != nil {
				if err := v.validateField(field.Name, value, rules); err != nil {
					result.AddError(field.Name, err.Error())
				}
			}
		}
	}

	result.Duration = time.Since(start)
	return result, nil
}

func (v *SchemaValidator) validateField(name string, value interface{}, rules *FieldRules) error {
	if rules.Type != "" {
		if !v.checkType(value, rules.Type) {
			return fmt.Errorf("expected type %s", rules.Type)
		}
	}

	if rules.MinLength != nil {
		if str, ok := value.(string); ok {
			if len(str) < *rules.MinLength {
				return fmt.Errorf("length must be at least %d", *rules.MinLength)
			}
		}
	}

	if rules.MaxLength != nil {
		if str, ok := value.(string); ok {
			if len(str) > *rules.MaxLength {
				return fmt.Errorf("length must be at most %d", *rules.MaxLength)
			}
		}
	}

	if rules.MinValue != nil {
		if num, ok := toFloat64(value); ok {
			if num < *rules.MinValue {
				return fmt.Errorf("value must be at least %f", *rules.MinValue)
			}
		}
	}

	if rules.MaxValue != nil {
		if num, ok := toFloat64(value); ok {
			if num > *rules.MaxValue {
				return fmt.Errorf("value must be at most %f", *rules.MaxValue)
			}
		}
	}

	if rules.Pattern != nil {
		if str, ok := value.(string); ok {
			if !rules.Pattern.MatchString(str) {
				return fmt.Errorf("value does not match pattern")
			}
		}
	}

	if rules.Enum != nil && len(rules.Enum) > 0 {
		if !v.isInEnum(value, rules.Enum) {
			return fmt.Errorf("value must be one of %v", rules.Enum)
		}
	}

	if rules.CustomFunc != nil {
		return rules.CustomFunc(value)
	}

	return nil
}

func (v *SchemaValidator) checkType(value interface{}, expectedType string) bool {
	if value == nil {
		return expectedType == "null" || expectedType == "nil"
	}

	switch expectedType {
	case "string":
		_, ok := value.(string)
		return ok
	case "int", "integer":
		switch value.(type) {
		case int, int8, int16, int32, int64:
			return true
		case float64:
			return math.Floor(value.(float64)) == value.(float64)
		}
		return false
	case "float", "double":
		_, ok := toFloat64(value)
		return ok
	case "bool", "boolean":
		_, ok := value.(bool)
		return ok
	case "array", "slice":
		_, ok := value.([]interface{})
		return ok
	case "object", "map":
		_, ok := value.(map[string]interface{})
		return ok
	}

	return reflect.TypeOf(value).Name() == expectedType
}

func (v *SchemaValidator) isInEnum(value interface{}, enum []interface{}) bool {
	for _, e := range enum {
		if fmt.Sprintf("%v", value) == fmt.Sprintf("%v", e) {
			return true
		}
	}
	return false
}

func (v *SchemaValidator) Type() ValidationType {
	return ValidationTypeSchema
}

func (v *SchemaValidator) Name() string {
	return "schema_validator"
}

func toFloat64(value interface{}) (float64, bool) {
	switch v := value.(type) {
	case float64:
		return v, true
	case float32:
		return float64(v), true
	case int:
		return float64(v), true
	case int8:
		return float64(v), true
	case int16:
		return float64(v), true
	case int32:
		return float64(v), true
	case int64:
		return float64(v), true
	case string:
		f, err := strconv.ParseFloat(v, 64)
		if err != nil {
			return 0, false
		}
		return f, true
	default:
		return 0, false
	}
}

type ChecksumValidator struct {
	mu         sync.RWMutex
	algorithm  string
	hashFunc   func() hash.Hash
}

func NewChecksumValidator(algorithm string) (*ChecksumValidator, error) {
	var hashFunc func() hash.Hash

	switch strings.ToLower(algorithm) {
	case "md5":
		hashFunc = md5.New
	case "sha1", "sha-1":
		hashFunc = sha1.New
	case "sha256", "sha-256":
		hashFunc = sha256.New
	default:
		return nil, errors.Newf(errors.ErrCodeInvalidParameter, "unsupported hash algorithm: %s", algorithm)
	}

	return &ChecksumValidator{
		algorithm: strings.ToLower(algorithm),
		hashFunc:  hashFunc,
	}, nil
}

func (v *ChecksumValidator) Validate(ctx context.Context, data interface{}) (*ValidationResult, error) {
	result := NewValidationResult()
	start := time.Now()

	var input []byte
	var err error

	switch d := data.(type) {
	case []byte:
		input = d
	case string:
		input = []byte(d)
	case io.Reader:
		input, err = io.ReadAll(d)
		if err != nil {
			result.AddError("data", fmt.Sprintf("failed to read data: %v", err))
			result.Duration = time.Since(start)
			return result, nil
		}
	default:
		result.AddError("data", "unsupported data type")
		result.Duration = time.Since(start)
		return result, nil
	}

	result.Metadata["algorithm"] = v.algorithm
	result.Metadata["input_size"] = len(input)

	hash := v.hashFunc()
	hash.Write(input)
	checksum := hex.EncodeToString(hash.Sum(nil))

	result.Metadata["checksum"] = checksum
	result.Valid = true

	result.Duration = time.Since(start)
	return result, nil
}

func (v *ChecksumValidator) ValidateWithChecksum(ctx context.Context, data interface{}, expectedChecksum string) (*ValidationResult, error) {
	result, err := v.Validate(ctx, data)
	if err != nil {
		return result, err
	}

	actualChecksum, ok := result.Metadata["checksum"].(string)
	if !ok {
		result.AddError("checksum", "failed to get actual checksum")
		return result, nil
	}

	if !strings.EqualFold(actualChecksum, expectedChecksum) {
		result.AddError("checksum", fmt.Sprintf("checksum mismatch: expected %s, got %s", expectedChecksum, actualChecksum))
	}

	return result, nil
}

func (v *ChecksumValidator) Type() ValidationType {
	return ValidationTypeChecksum
}

func (v *ChecksumValidator) Name() string {
	return "checksum_validator"
}

func (v *ChecksumValidator) CalculateChecksum(data []byte) string {
	hash := v.hashFunc()
	hash.Write(data)
	return hex.EncodeToString(hash.Sum(nil))
}

type QualityValidator struct {
	mu          sync.RWMutex
	minCompleteness float64
	minAccuracy     float64
	nullThreshold   float64
}

type QualityRules struct {
	MinCompleteness float64
	MinAccuracy     float64
	NullThreshold   float64
	AllowDuplicates bool
	UniqueFields    []string
}

func NewQualityValidator(rules *QualityRules) *QualityValidator {
	if rules == nil {
		rules = &QualityRules{
			MinCompleteness: 0.8,
			MinAccuracy:      0.9,
			NullThreshold:    0.1,
			AllowDuplicates: true,
		}
	}

	return &QualityValidator{
		minCompleteness: rules.MinCompleteness,
		minAccuracy:     rules.MinAccuracy,
		nullThreshold:   rules.NullThreshold,
	}
}

func (v *QualityValidator) Validate(ctx context.Context, data interface{}) (*ValidationResult, error) {
	v.mu.RLock()
	defer v.mu.RUnlock()

	result := NewValidationResult()
	start := time.Now()

	dataList, ok := data.([]interface{})
	if !ok {
		singleData, ok := data.(map[string]interface{})
		if !ok {
			jsonData, err := json.Marshal(data)
			if err != nil {
				result.AddError("data", "failed to marshal data")
				result.Duration = time.Since(start)
				return result, nil
			}
			if err := json.Unmarshal(jsonData, &singleData); err != nil {
				result.AddError("data", "failed to unmarshal data to map")
				result.Duration = time.Since(start)
				return result, nil
			}
			dataList = []interface{}{singleData}
		} else {
			dataList = []interface{}{singleData}
		}
	}

	if len(dataList) == 0 {
		result.AddError("data", "data is empty")
		result.Duration = time.Since(start)
		return result, nil
	}

	completeness := v.calculateCompleteness(dataList)
	accuracy := v.calculateAccuracy(dataList)
	nullPercent := v.calculateNullPercent(dataList)

	result.Metadata["completeness"] = completeness
	result.Metadata["accuracy"] = accuracy
	result.Metadata["null_percent"] = nullPercent
	result.Metadata["record_count"] = len(dataList)

	if completeness < v.minCompleteness {
		result.AddError("completeness", fmt.Sprintf("completeness %.2f is below threshold %.2f", completeness, v.minCompleteness))
	}

	if accuracy < v.minAccuracy {
		result.AddError("accuracy", fmt.Sprintf("accuracy %.2f is below threshold %.2f", accuracy, v.minAccuracy))
	}

	if nullPercent > v.nullThreshold {
		result.AddWarning("null_percent", fmt.Sprintf("null percent %.2f exceeds threshold %.2f", nullPercent, v.nullThreshold))
	}

	result.Valid = len(result.Errors) == 0
	result.Duration = time.Since(start)

	return result, nil
}

func (v *QualityValidator) calculateCompleteness(dataList []interface{}) float64 {
	if len(dataList) == 0 {
		return 0
	}

	fieldCounts := make(map[string]int)
	totalFields := 0

	for _, item := range dataList {
		if m, ok := item.(map[string]interface{}); ok {
			for k, val := range m {
				totalFields++
				if val != nil {
					fieldCounts[k]++
				}
			}
		}
	}

	if totalFields == 0 {
		return 0
	}

	nonNullCount := 0
	for _, count := range fieldCounts {
		nonNullCount += count
	}

	return float64(nonNullCount) / float64(totalFields)
}

func (v *QualityValidator) calculateAccuracy(dataList []interface{}) float64 {
	if len(dataList) == 0 {
		return 0
	}

	validCount := 0

	for _, item := range dataList {
		if m, ok := item.(map[string]interface{}); ok {
			isValid := true
			for _, val := range m {
				if val == nil {
					continue
				}
				switch v := val.(type) {
				case string:
					if v == "" {
						isValid = false
						break
					}
				case float64:
					if math.IsNaN(v) || math.IsInf(v, 0) {
						isValid = false
						break
					}
				}
			}
			if isValid {
				validCount++
			}
		}
	}

	return float64(validCount) / float64(len(dataList))
}

func (v *QualityValidator) calculateNullPercent(dataList []interface{}) float64 {
	if len(dataList) == 0 {
		return 0
	}

	totalFields := 0
	nullCount := 0

	for _, item := range dataList {
		if m, ok := item.(map[string]interface{}); ok {
			for _, val := range m {
				totalFields++
				if val == nil {
					nullCount++
				}
			}
		}
	}

	if totalFields == 0 {
		return 0
	}

	return float64(nullCount) / float64(totalFields)
}

func (v *QualityValidator) Type() ValidationType {
	return ValidationTypeQuality
}

func (v *QualityValidator) Name() string {
	return "quality_validator"
}

type ValidatorChain struct {
	mu        sync.RWMutex
	validators []Validator
}

func NewValidatorChain() *ValidatorChain {
	return &ValidatorChain{
		validators: make([]Validator, 0),
	}
}

func (c *ValidatorChain) Add(validator Validator) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.validators = append(c.validators, validator)
}

func (c *ValidatorChain) Validate(ctx context.Context, data interface{}) (*ValidationResult, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	result := NewValidationResult()

	for _, validator := range c.validators {
		validatorResult, err := validator.Validate(ctx, data)
		if err != nil {
			result.AddError(validator.Name(), fmt.Sprintf("validation failed: %v", err))
			continue
		}

		result.Merge(validatorResult)
	}

	return result, nil
}

func (c *ValidatorChain) ValidateWithBreak(ctx context.Context, data interface{}) (*ValidationResult, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	result := NewValidationResult()

	for _, validator := range c.validators {
		validatorResult, err := validator.Validate(ctx, data)
		if err != nil {
			result.AddError(validator.Name(), fmt.Sprintf("validation failed: %v", err))
			break
		}

		result.Merge(validatorResult)

		if !validatorResult.Valid {
			break
		}
	}

	return result, nil
}

type ValidatorConfig struct {
	Type          ValidationType `json:"type"`
	Schema        *models.Schema `json:"schema,omitempty"`
	Rules         map[string]*FieldRules `json:"rules,omitempty"`
	Algorithm     string          `json:"algorithm,omitempty"`
	QualityRules  *QualityRules   `json:"quality_rules,omitempty"`
}

func NewValidatorFromConfig(config *ValidatorConfig) (Validator, error) {
	switch config.Type {
	case ValidationTypeSchema:
		if config.Schema == nil {
			return nil, errors.New(errors.ErrCodeInvalidConfiguration, "schema is required for schema validator")
		}
		validator := NewSchemaValidator(config.Schema)
		if config.Rules != nil {
			for fieldName, rules := range config.Rules {
				validator.AddRules(fieldName, rules)
			}
		}
		return validator, nil

	case ValidationTypeChecksum:
		if config.Algorithm == "" {
			config.Algorithm = "md5"
		}
		return NewChecksumValidator(config.Algorithm)

	case ValidationTypeQuality:
		return NewQualityValidator(config.QualityRules), nil

	default:
		return nil, errors.Newf(errors.ErrCodeInvalidParameter, "unsupported validation type: %s", config.Type)
	}
}

type ValidatorRegistry struct {
	mu         sync.RWMutex
	validators map[string]Validator
}

func NewValidatorRegistry() *ValidatorRegistry {
	return &ValidatorRegistry{
		validators: make(map[string]Validator),
	}
}

func (r *ValidatorRegistry) Register(name string, validator Validator) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.validators[name] = validator
}

func (r *ValidatorRegistry) Get(name string) (Validator, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	v, ok := r.validators[name]
	return v, ok
}

func (r *ValidatorRegistry) CreateAndRegister(name string, config *ValidatorConfig) error {
	validator, err := NewValidatorFromConfig(config)
	if err != nil {
		return err
	}
	r.Register(name, validator)
	return nil
}

func ValidateFileChecksum(ctx context.Context, filePath string, expectedChecksum string, algorithm string) (*ValidationResult, error) {
	result := NewValidationResult()
	start := time.Now()

	validator, err := NewChecksumValidator(algorithm)
	if err != nil {
		result.AddError("validator", err.Error())
		result.Duration = time.Since(start)
		return result, err
	}

	file, err := os.Open(filePath)
	if err != nil {
		result.AddError("file", fmt.Sprintf("failed to open file: %v", err))
		result.Duration = time.Since(start)
		return result, nil
	}
	defer file.Close()

	fileInfo, err := file.Stat()
	if err != nil {
		result.AddError("file", fmt.Sprintf("failed to stat file: %v", err))
		result.Duration = time.Since(start)
		return result, nil
	}

	result.Metadata["file_size"] = fileInfo.Size()
	result.Metadata["file_path"] = filePath

	hash := validator.hashFunc()
	if _, err := io.Copy(hash, file); err != nil {
		result.AddError("file", fmt.Sprintf("failed to calculate checksum: %v", err))
		result.Duration = time.Since(start)
		return result, nil
	}

	actualChecksum := hex.EncodeToString(hash.Sum(nil))
	result.Metadata["checksum"] = actualChecksum

	if !strings.EqualFold(actualChecksum, expectedChecksum) {
		result.AddError("checksum", fmt.Sprintf("checksum mismatch: expected %s, got %s", expectedChecksum, actualChecksum))
	} else {
		result.Valid = true
	}

	result.Duration = time.Since(start)
	return result, nil
}

func ValidateJSONSchema(data []byte, schema *models.Schema) (*ValidationResult, error) {
	validator := NewSchemaValidator(schema)

	var jsonData interface{}
	if err := json.Unmarshal(data, &jsonData); err != nil {
		result := NewValidationResult()
		result.AddError("json", fmt.Sprintf("failed to parse JSON: %v", err))
		return result, nil
	}

	return validator.Validate(context.Background(), jsonData)
}

type DataValidator struct {
	schemaValidator   *SchemaValidator
	checksumValidator *ChecksumValidator
	qualityValidator  *QualityValidator
}

func NewDataValidator(schema *models.Schema, algorithm string, qualityRules *QualityRules) (*DataValidator, error) {
	schemaValidator := NewSchemaValidator(schema)

	checksumValidator, err := NewChecksumValidator(algorithm)
	if err != nil {
		return nil, err
	}

	qualityValidator := NewQualityValidator(qualityRules)

	return &DataValidator{
		schemaValidator:   schemaValidator,
		checksumValidator: checksumValidator,
		qualityValidator:  qualityValidator,
	}, nil
}

func (v *DataValidator) ValidateAll(ctx context.Context, data interface{}) (*ValidationResult, error) {
	result := NewValidationResult()

	schemaResult, err := v.schemaValidator.Validate(ctx, data)
	if err != nil {
		result.AddError("schema", fmt.Sprintf("validation failed: %v", err))
	} else {
		result.Merge(schemaResult)
	}

	checksumResult, err := v.checksumValidator.Validate(ctx, data)
	if err != nil {
		result.AddError("checksum", fmt.Sprintf("validation failed: %v", err))
	} else {
		result.Merge(checksumResult)
	}

	qualityResult, err := v.qualityValidator.Validate(ctx, data)
	if err != nil {
		result.AddError("quality", fmt.Sprintf("validation failed: %v", err))
	} else {
		result.Merge(qualityResult)
	}

	return result, nil
}
