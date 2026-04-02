package unit

import (
	"context"
	"regexp"
	"testing"

	"datapipe/internal/models"
	"datapipe/internal/validation"
)

func TestSchemaValidator(t *testing.T) {
	t.Run("basic validation", func(t *testing.T) {
		schema := &models.Schema{
			Fields: []models.Field{
				{Name: "name", Type: "string"},
				{Name: "age", Type: "int"},
			},
		}

		validator := validation.NewSchemaValidator(schema)

		data := map[string]interface{}{
			"name": "test",
			"age":  25,
		}

		result, err := validator.Validate(context.Background(), data)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if !result.Valid {
			t.Errorf("expected valid result, got errors: %v", result.Errors)
		}
	})

	t.Run("missing required field", func(t *testing.T) {
		schema := &models.Schema{
			Fields: []models.Field{
				{Name: "name", Type: "string"},
				{Name: "age", Type: "int"},
			},
		}

		validator := validation.NewSchemaValidator(schema)

		minLength := 1
		validator.AddRules("name", &validation.FieldRules{
			Required:  true,
			MinLength: &minLength,
		})

		data := map[string]interface{}{
			"age": 25,
		}

		result, err := validator.Validate(context.Background(), data)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if result.Valid {
			t.Error("expected invalid result for missing required field")
		}
	})

	t.Run("type validation", func(t *testing.T) {
		schema := &models.Schema{
			Fields: []models.Field{
				{Name: "name", Type: "string"},
			},
		}

		validator := validation.NewSchemaValidator(schema)

		data := map[string]interface{}{
			"name": 123,
		}

		result, err := validator.Validate(context.Background(), data)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if result.Valid {
			t.Error("expected invalid result for type mismatch")
		}
	})

	t.Run("min length validation", func(t *testing.T) {
		schema := &models.Schema{
			Fields: []models.Field{
				{Name: "name", Type: "string"},
			},
		}

		validator := validation.NewSchemaValidator(schema)

		minLength := 5
		validator.AddRules("name", &validation.FieldRules{
			MinLength: &minLength,
		})

		data := map[string]interface{}{
			"name": "ab",
		}

		result, err := validator.Validate(context.Background(), data)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if result.Valid {
			t.Error("expected invalid result for string too short")
		}
	})

	t.Run("max length validation", func(t *testing.T) {
		schema := &models.Schema{
			Fields: []models.Field{
				{Name: "code", Type: "string"},
			},
		}

		validator := validation.NewSchemaValidator(schema)

		maxLength := 5
		validator.AddRules("code", &validation.FieldRules{
			MaxLength: &maxLength,
		})

		data := map[string]interface{}{
			"code": "abcdef",
		}

		result, err := validator.Validate(context.Background(), data)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if result.Valid {
			t.Error("expected invalid result for string too long")
		}
	})

	t.Run("min value validation", func(t *testing.T) {
		schema := &models.Schema{
			Fields: []models.Field{
				{Name: "age", Type: "int"},
			},
		}

		validator := validation.NewSchemaValidator(schema)

		minValue := 0.0
		validator.AddRules("age", &validation.FieldRules{
			MinValue: &minValue,
		})

		data := map[string]interface{}{
			"age": -5,
		}

		result, err := validator.Validate(context.Background(), data)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if result.Valid {
			t.Error("expected invalid result for value below minimum")
		}
	})

	t.Run("max value validation", func(t *testing.T) {
		schema := &models.Schema{
			Fields: []models.Field{
				{Name: "score", Type: "float"},
			},
		}

		validator := validation.NewSchemaValidator(schema)

		maxValue := 100.0
		validator.AddRules("score", &validation.FieldRules{
			MaxValue: &maxValue,
		})

		data := map[string]interface{}{
			"score": 150.0,
		}

		result, err := validator.Validate(context.Background(), data)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if result.Valid {
			t.Error("expected invalid result for value above maximum")
		}
	})

	t.Run("pattern validation", func(t *testing.T) {
		schema := &models.Schema{
			Fields: []models.Field{
				{Name: "email", Type: "string"},
			},
		}

		validator := validation.NewSchemaValidator(schema)

		pattern := regexp.MustCompile(`^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$`)
		validator.AddRules("email", &validation.FieldRules{
			Pattern: pattern,
		})

		data := map[string]interface{}{
			"email": "invalid-email",
		}

		result, err := validator.Validate(context.Background(), data)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if result.Valid {
			t.Error("expected invalid result for pattern mismatch")
		}
	})

	t.Run("enum validation", func(t *testing.T) {
		schema := &models.Schema{
			Fields: []models.Field{
				{Name: "status", Type: "string"},
			},
		}

		validator := validation.NewSchemaValidator(schema)

		validator.AddRules("status", &validation.FieldRules{
			Enum: []interface{}{"active", "inactive", "pending"},
		})

		data := map[string]interface{}{
			"status": "unknown",
		}

		result, err := validator.Validate(context.Background(), data)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if result.Valid {
			t.Error("expected invalid result for enum value not in list")
		}
	})

	t.Run("nil data", func(t *testing.T) {
		schema := &models.Schema{
			Fields: []models.Field{
				{Name: "name", Type: "string"},
			},
		}

		validator := validation.NewSchemaValidator(schema)

		result, err := validator.Validate(context.Background(), nil)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if result.Valid {
			t.Error("expected invalid result for nil data")
		}
	})

	t.Run("validator type and name", func(t *testing.T) {
		schema := &models.Schema{
			Fields: []models.Field{
				{Name: "name", Type: "string"},
			},
		}

		validator := validation.NewSchemaValidator(schema)

		if validator.Type() != validation.ValidationTypeSchema {
			t.Errorf("expected type 'schema', got '%s'", validator.Type())
		}
		if validator.Name() != "schema_validator" {
			t.Errorf("expected name 'schema_validator', got '%s'", validator.Name())
		}
	})
}

func TestChecksumValidator(t *testing.T) {
	t.Run("md5 checksum", func(t *testing.T) {
		validator, err := validation.NewChecksumValidator("md5")
		if err != nil {
			t.Fatalf("failed to create md5 validator: %v", err)
		}

		data := []byte("hello world")

		result, err := validator.Validate(context.Background(), data)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if !result.Valid {
			t.Errorf("expected valid result, got errors: %v", result.Errors)
		}

		checksum, ok := result.Metadata["checksum"].(string)
		if !ok {
			t.Fatal("expected checksum in metadata")
		}

		if len(checksum) != 32 {
			t.Errorf("expected md5 checksum length 32, got %d", len(checksum))
		}
	})

	t.Run("sha256 checksum", func(t *testing.T) {
		validator, err := validation.NewChecksumValidator("sha256")
		if err != nil {
			t.Fatalf("failed to create sha256 validator: %v", err)
		}

		data := []byte("hello world")

		result, err := validator.Validate(context.Background(), data)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		checksum, ok := result.Metadata["checksum"].(string)
		if !ok {
			t.Fatal("expected checksum in metadata")
		}

		if len(checksum) != 64 {
			t.Errorf("expected sha256 checksum length 64, got %d", len(checksum))
		}
	})

	t.Run("sha1 checksum", func(t *testing.T) {
		validator, err := validation.NewChecksumValidator("sha1")
		if err != nil {
			t.Fatalf("failed to create sha1 validator: %v", err)
		}

		data := []byte("hello world")

		result, err := validator.Validate(context.Background(), data)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		checksum, ok := result.Metadata["checksum"].(string)
		if !ok {
			t.Fatal("expected checksum in metadata")
		}

		if len(checksum) != 40 {
			t.Errorf("expected sha1 checksum length 40, got %d", len(checksum))
		}
	})

	t.Run("string input", func(t *testing.T) {
		validator, err := validation.NewChecksumValidator("md5")
		if err != nil {
			t.Fatalf("failed to create validator: %v", err)
		}

		result, err := validator.Validate(context.Background(), "test string")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if !result.Valid {
			t.Errorf("expected valid result, got errors: %v", result.Errors)
		}
	})

	t.Run("validate with matching checksum", func(t *testing.T) {
		validator, err := validation.NewChecksumValidator("md5")
		if err != nil {
			t.Fatalf("failed to create validator: %v", err)
		}

		data := []byte("hello world")
		expectedChecksum := validator.CalculateChecksum(data)

		result, err := validator.ValidateWithChecksum(context.Background(), data, expectedChecksum)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if !result.Valid {
			t.Errorf("expected valid result for matching checksum, got errors: %v", result.Errors)
		}
	})

	t.Run("validate with mismatching checksum", func(t *testing.T) {
		validator, err := validation.NewChecksumValidator("md5")
		if err != nil {
			t.Fatalf("failed to create validator: %v", err)
		}

		data := []byte("hello world")

		result, err := validator.ValidateWithChecksum(context.Background(), data, "wrongchecksum")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if result.Valid {
			t.Error("expected invalid result for mismatching checksum")
		}
	})

	t.Run("unsupported algorithm", func(t *testing.T) {
		_, err := validation.NewChecksumValidator("unsupported")
		if err == nil {
			t.Error("expected error for unsupported algorithm")
		}
	})

	t.Run("validator type and name", func(t *testing.T) {
		validator, _ := validation.NewChecksumValidator("md5")

		if validator.Type() != validation.ValidationTypeChecksum {
			t.Errorf("expected type 'checksum', got '%s'", validator.Type())
		}
		if validator.Name() != "checksum_validator" {
			t.Errorf("expected name 'checksum_validator', got '%s'", validator.Name())
		}
	})
}

func TestValidatorChain(t *testing.T) {
	t.Run("multiple validators", func(t *testing.T) {
		schema := &models.Schema{
			Fields: []models.Field{
				{Name: "name", Type: "string"},
			},
		}

		schemaValidator := validation.NewSchemaValidator(schema)
		checksumValidator, _ := validation.NewChecksumValidator("md5")

		chain := validation.NewValidatorChain()
		chain.Add(schemaValidator)
		chain.Add(checksumValidator)

		data := map[string]interface{}{
			"name": "test",
		}

		result, err := chain.Validate(context.Background(), data)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if !result.Valid {
			t.Errorf("expected valid result, got errors: %v", result.Errors)
		}
	})

	t.Run("validation breaks on error", func(t *testing.T) {
		schema := &models.Schema{
			Fields: []models.Field{
				{Name: "name", Type: "string"},
			},
		}

		schemaValidator := validation.NewSchemaValidator(schema)

		chain := validation.NewValidatorChain()
		chain.Add(schemaValidator)

		invalidData := map[string]interface{}{
			"name": 123,
		}

		result, err := chain.ValidateWithBreak(context.Background(), invalidData)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if result.Valid {
			t.Error("expected invalid result")
		}
	})
}

func TestValidatorRegistry(t *testing.T) {
	t.Run("register and get", func(t *testing.T) {
		registry := validation.NewValidatorRegistry()

		schema := &models.Schema{
			Fields: []models.Field{
				{Name: "name", Type: "string"},
			},
		}

		validator := validation.NewSchemaValidator(schema)
		registry.Register("test-schema", validator)

		retrieved, ok := registry.Get("test-schema")
		if !ok {
			t.Fatal("expected to get registered validator")
		}
		if retrieved.Name() != "schema_validator" {
			t.Errorf("expected 'schema_validator', got '%s'", retrieved.Name())
		}
	})

	t.Run("get non-existent", func(t *testing.T) {
		registry := validation.NewValidatorRegistry()

		_, ok := registry.Get("non-existent")
		if ok {
			t.Error("expected not found for non-existent validator")
		}
	})

	t.Run("create and register from config", func(t *testing.T) {
		registry := validation.NewValidatorRegistry()

		schema := &models.Schema{
			Fields: []models.Field{
				{Name: "name", Type: "string"},
			},
		}

		err := registry.CreateAndRegister("md5-checksum", &validation.ValidatorConfig{
			Type:      validation.ValidationTypeChecksum,
			Algorithm: "md5",
			Schema:    schema,
		})
		if err != nil {
			t.Fatalf("failed to create and register: %v", err)
		}

		retrieved, ok := registry.Get("md5-checksum")
		if !ok {
			t.Fatal("expected to get registered validator")
		}
		if retrieved.Name() != "checksum_validator" {
			t.Errorf("expected 'checksum_validator', got '%s'", retrieved.Name())
		}
	})
}

func TestQualityValidator(t *testing.T) {
	t.Run("completeness calculation", func(t *testing.T) {
		rules := &validation.QualityRules{
			MinCompleteness: 0.5,
			AllowDuplicates:  true,
		}

		validator := validation.NewQualityValidator(rules)

		data := []interface{}{
			map[string]interface{}{"name": "alice", "age": 30},
			map[string]interface{}{"name": "bob", "age": nil},
		}

		result, err := validator.Validate(context.Background(), data)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		completeness, ok := result.Metadata["completeness"].(float64)
		if !ok {
			t.Fatal("expected completeness in metadata")
		}

		if completeness < 0.5 {
			t.Errorf("expected completeness >= 0.5, got %f", completeness)
		}
	})
}

func TestValidationResult(t *testing.T) {
	t.Run("add error", func(t *testing.T) {
		result := validation.NewValidationResult()

		result.AddError("field1", "error message")

		if result.Valid {
			t.Error("expected Valid to be false after adding error")
		}
		if len(result.Errors) != 1 {
			t.Errorf("expected 1 error, got %d", len(result.Errors))
		}
	})

	t.Run("add warning", func(t *testing.T) {
		result := validation.NewValidationResult()

		result.AddWarning("field1", "warning message")

		if result.Valid {
			t.Error("expected Valid to be false after adding warning")
		}
		if len(result.Warnings) != 1 {
			t.Errorf("expected 1 warning, got %d", len(result.Warnings))
		}
	})

	t.Run("merge results", func(t *testing.T) {
		result1 := validation.NewValidationResult()
		result1.AddError("field1", "error1")

		result2 := validation.NewValidationResult()
		result2.AddError("field2", "error2")
		result2.AddWarning("field3", "warning1")

		result1.Merge(result2)

		if len(result1.Errors) != 2 {
			t.Errorf("expected 2 errors after merge, got %d", len(result1.Errors))
		}
		if len(result1.Warnings) != 1 {
			t.Errorf("expected 1 warning after merge, got %d", len(result1.Warnings))
		}
	})
}

func TestValidationErrorAndWarning(t *testing.T) {
	t.Run("validation error message", func(t *testing.T) {
		err := &validation.ValidationError{
			Field:   "email",
			Message: "invalid format",
			Code:    "INVALID_FORMAT",
		}

		expected := "[INVALID_FORMAT] email: invalid format"
		if err.Error() != expected {
			t.Errorf("expected '%s', got '%s'", expected, err.Error())
		}
	})

	t.Run("validation warning message", func(t *testing.T) {
		warn := &validation.ValidationWarning{
			Field:   "age",
			Message: "unusual value",
			Code:    "UNUSUAL_VALUE",
		}

		expected := "[UNUSUAL_VALUE] age: unusual value"
		if warn.Error() != expected {
			t.Errorf("expected '%s', got '%s'", expected, warn.Error())
		}
	})
}

func TestValidateJSONSchema(t *testing.T) {
	schema := &models.Schema{
		Fields: []models.Field{
			{Name: "name", Type: "string"},
			{Name: "age", Type: "int"},
		},
	}

	jsonData := []byte(`{"name": "test", "age": 25}`)

	result, err := validation.ValidateJSONSchema(jsonData, schema)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !result.Valid {
		t.Errorf("expected valid result, got errors: %v", result.Errors)
	}
}

func TestDataValidator(t *testing.T) {
	t.Run("validate all", func(t *testing.T) {
		schema := &models.Schema{
			Fields: []models.Field{
				{Name: "name", Type: "string"},
			},
		}

		validator, err := validation.NewDataValidator(schema, "md5", nil)
		if err != nil {
			t.Fatalf("failed to create data validator: %v", err)
		}

		data := map[string]interface{}{
			"name": "test",
		}

		result, err := validator.ValidateAll(context.Background(), data)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if !result.Valid {
			t.Errorf("expected valid result, got errors: %v", result.Errors)
		}
	})
}
