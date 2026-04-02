package models

import (
	"database/sql/driver"
	"encoding/json"
	"errors"
	"time"
)

type FunctionType string

const (
	FunctionTypeStart  FunctionType = "start"
	FunctionTypeNormal FunctionType = "normal"
	FunctionTypeEnd    FunctionType = "end"
)

type FunctionStatus string

const (
	FunctionStatusRegistered FunctionStatus = "registered"
	FunctionStatusActive     FunctionStatus = "active"
	FunctionStatusDeprecated FunctionStatus = "deprecated"
)

type FunctionDefinition struct {
	InputType  InputType   `json:"input_type"`
	OutputType OutputType  `json:"output_type"`
	Config     interface{} `json:"config"`
}

type InputType struct {
	Type   string `json:"type"`
	Schema Schema `json:"schema,omitempty"`
}

type OutputType struct {
	Type   string `json:"type"`
	Schema Schema `json:"schema,omitempty"`
}

type Schema struct {
	Fields []Field `json:"fields"`
}

type Field struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

func (f FunctionDefinition) Value() (driver.Value, error) {
	return json.Marshal(f)
}

func (f *FunctionDefinition) Scan(value interface{}) error {
	if value == nil {
		return nil
	}
	bytes, ok := value.([]byte)
	if !ok {
		return errors.New("type assertion to []byte failed")
	}
	return json.Unmarshal(bytes, f)
}

type Function struct {
	ID         string           `gorm:"type:varchar(36);primaryKey" json:"id"`
	Name       string           `gorm:"type:varchar(255);uniqueIndex;not null" json:"name"`
	Type       FunctionType     `gorm:"type:varchar(20);not null;index:idx_type" json:"type"`
	Version    string           `gorm:"type:varchar(50);not null" json:"version"`
	Definition FunctionDefinition `gorm:"type:json;not null" json:"definition"`
	Image      string           `gorm:"type:varchar(512)" json:"image"`
	Status     FunctionStatus   `gorm:"type:varchar(20);default:'registered'" json:"status"`
	CreatedAt  time.Time        `gorm:"autoCreateTime" json:"created_at"`
	UpdatedAt  time.Time        `gorm:"autoUpdateTime" json:"updated_at"`
}

func (Function) TableName() string {
	return "functions"
}
