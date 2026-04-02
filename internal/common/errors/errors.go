package errors

import (
	"fmt"
	"strings"
)

type ErrorCode int

const (
	ErrCodeUnknown ErrorCode = iota

	ErrCodeInvalidParameter
	ErrCodeMissingParameter
	ErrCodeInvalidConfiguration
	ErrCodeConfigurationNotFound

	ErrCodePipelineNotFound
	ErrCodePipelineAlreadyExists
	ErrCodePipelineRunning
	ErrCodePipelineStopped
	ErrCodePipelineCompleted
	ErrCodePipelineFailed
	ErrCodePipelineTimeout

	ErrCodeNodeNotFound
	ErrCodeNodeAlreadyExists
	ErrCodeNodeNotReady
	ErrCodeNodeProcessing
	ErrCodeNodeFailed
	ErrCodeNodeTimeout

	ErrCodeTaskNotFound
	ErrCodeTaskAlreadyExists
	ErrCodeTaskPending
	ErrCodeTaskRunning
	ErrCodeTaskCompleted
	ErrCodeTaskFailed
	ErrCodeTaskCancelled
	ErrCodeTaskTimeout
	ErrCodeTaskRetryExhausted

	ErrCodeWorkerNotFound
	ErrCodeWorkerAlreadyExists
	ErrCodeWorkerNotAvailable
	ErrCodeWorkerHeartbeatTimeout
	ErrCodeWorkerCapacityExceeded

	ErrCodeDatabaseError
	ErrCodeDatabaseConnectionFailed
	ErrCodeDatabaseQueryFailed
	ErrCodeDatabaseTransactionFailed
	ErrCodeDatabaseDuplicateEntry
	ErrCodeDatabaseNotFound

	ErrCodeRedisError
	ErrCodeRedisConnectionFailed
	ErrCodeRedisOperationFailed
	ErrCodeRedisTimeout

	ErrCodeNetworkError
	ErrCodeConnectionRefused
	ErrCodeConnectionTimeout
	ErrCodeRequestTimeout

	ErrCodeUnauthorized
	ErrCodeForbidden
	ErrCodeNotFound
	ErrCodeConflict
	ErrCodeInternalServerError
	ErrCodeServiceUnavailable
	ErrCodeGatewayTimeout

	ErrCodeValidationFailed
	ErrCodeSerializationFailed
	ErrCodeDeserializationFailed

	ErrCodeFileNotFound
	ErrCodeFileReadFailed
	ErrCodeFileWriteFailed
	ErrCodeFilePermissionDenied

	ErrCodeResourceNotFound
	ErrCodeResourceAlreadyExists
	ErrCodeResourceExhausted
	ErrCodeResourceBusy

	ErrCodeOperationCancelled
	ErrCodeOperationTimeout
	ErrCodeOperationNotSupported

	ErrCodeSystemError
	ErrCodeOutOfMemory
	ErrCodeOutOfDiskSpace
	ErrCodeOutOfCapacity
)

func (e ErrorCode) String() string {
	switch e {
	case ErrCodeUnknown:
		return "UNKNOWN"

	case ErrCodeInvalidParameter:
		return "INVALID_PARAMETER"
	case ErrCodeMissingParameter:
		return "MISSING_PARAMETER"
	case ErrCodeInvalidConfiguration:
		return "INVALID_CONFIGURATION"
	case ErrCodeConfigurationNotFound:
		return "CONFIGURATION_NOT_FOUND"

	case ErrCodePipelineNotFound:
		return "PIPELINE_NOT_FOUND"
	case ErrCodePipelineAlreadyExists:
		return "PIPELINE_ALREADY_EXISTS"
	case ErrCodePipelineRunning:
		return "PIPELINE_RUNNING"
	case ErrCodePipelineStopped:
		return "PIPELINE_STOPPED"
	case ErrCodePipelineCompleted:
		return "PIPELINE_COMPLETED"
	case ErrCodePipelineFailed:
		return "PIPELINE_FAILED"
	case ErrCodePipelineTimeout:
		return "PIPELINE_TIMEOUT"

	case ErrCodeNodeNotFound:
		return "NODE_NOT_FOUND"
	case ErrCodeNodeAlreadyExists:
		return "NODE_ALREADY_EXISTS"
	case ErrCodeNodeNotReady:
		return "NODE_NOT_READY"
	case ErrCodeNodeProcessing:
		return "NODE_PROCESSING"
	case ErrCodeNodeFailed:
		return "NODE_FAILED"
	case ErrCodeNodeTimeout:
		return "NODE_TIMEOUT"

	case ErrCodeTaskNotFound:
		return "TASK_NOT_FOUND"
	case ErrCodeTaskAlreadyExists:
		return "TASK_ALREADY_EXISTS"
	case ErrCodeTaskPending:
		return "TASK_PENDING"
	case ErrCodeTaskRunning:
		return "TASK_RUNNING"
	case ErrCodeTaskCompleted:
		return "TASK_COMPLETED"
	case ErrCodeTaskFailed:
		return "TASK_FAILED"
	case ErrCodeTaskCancelled:
		return "TASK_CANCELLED"
	case ErrCodeTaskTimeout:
		return "TASK_TIMEOUT"
	case ErrCodeTaskRetryExhausted:
		return "TASK_RETRY_EXHAUSTED"

	case ErrCodeWorkerNotFound:
		return "WORKER_NOT_FOUND"
	case ErrCodeWorkerAlreadyExists:
		return "WORKER_ALREADY_EXISTS"
	case ErrCodeWorkerNotAvailable:
		return "WORKER_NOT_AVAILABLE"
	case ErrCodeWorkerHeartbeatTimeout:
		return "WORKER_HEARTBEAT_TIMEOUT"
	case ErrCodeWorkerCapacityExceeded:
		return "WORKER_CAPACITY_EXCEEDED"

	case ErrCodeDatabaseError:
		return "DATABASE_ERROR"
	case ErrCodeDatabaseConnectionFailed:
		return "DATABASE_CONNECTION_FAILED"
	case ErrCodeDatabaseQueryFailed:
		return "DATABASE_QUERY_FAILED"
	case ErrCodeDatabaseTransactionFailed:
		return "DATABASE_TRANSACTION_FAILED"
	case ErrCodeDatabaseDuplicateEntry:
		return "DATABASE_DUPLICATE_ENTRY"
	case ErrCodeDatabaseNotFound:
		return "DATABASE_NOT_FOUND"

	case ErrCodeRedisError:
		return "REDIS_ERROR"
	case ErrCodeRedisConnectionFailed:
		return "REDIS_CONNECTION_FAILED"
	case ErrCodeRedisOperationFailed:
		return "REDIS_OPERATION_FAILED"
	case ErrCodeRedisTimeout:
		return "REDIS_TIMEOUT"

	case ErrCodeNetworkError:
		return "NETWORK_ERROR"
	case ErrCodeConnectionRefused:
		return "CONNECTION_REFUSED"
	case ErrCodeConnectionTimeout:
		return "CONNECTION_TIMEOUT"
	case ErrCodeRequestTimeout:
		return "REQUEST_TIMEOUT"

	case ErrCodeUnauthorized:
		return "UNAUTHORIZED"
	case ErrCodeForbidden:
		return "FORBIDDEN"
	case ErrCodeNotFound:
		return "NOT_FOUND"
	case ErrCodeConflict:
		return "CONFLICT"
	case ErrCodeInternalServerError:
		return "INTERNAL_SERVER_ERROR"
	case ErrCodeServiceUnavailable:
		return "SERVICE_UNAVAILABLE"
	case ErrCodeGatewayTimeout:
		return "GATEWAY_TIMEOUT"

	case ErrCodeValidationFailed:
		return "VALIDATION_FAILED"
	case ErrCodeSerializationFailed:
		return "SERIALIZATION_FAILED"
	case ErrCodeDeserializationFailed:
		return "DESERIALIZATION_FAILED"

	case ErrCodeFileNotFound:
		return "FILE_NOT_FOUND"
	case ErrCodeFileReadFailed:
		return "FILE_READ_FAILED"
	case ErrCodeFileWriteFailed:
		return "FILE_WRITE_FAILED"
	case ErrCodeFilePermissionDenied:
		return "FILE_PERMISSION_DENIED"

	case ErrCodeResourceNotFound:
		return "RESOURCE_NOT_FOUND"
	case ErrCodeResourceAlreadyExists:
		return "RESOURCE_ALREADY_EXISTS"
	case ErrCodeResourceExhausted:
		return "RESOURCE_EXHAUSTED"
	case ErrCodeResourceBusy:
		return "RESOURCE_BUSY"

	case ErrCodeOperationCancelled:
		return "OPERATION_CANCELLED"
	case ErrCodeOperationTimeout:
		return "OPERATION_TIMEOUT"
	case ErrCodeOperationNotSupported:
		return "OPERATION_NOT_SUPPORTED"

	case ErrCodeSystemError:
		return "SYSTEM_ERROR"
	case ErrCodeOutOfMemory:
		return "OUT_OF_MEMORY"
	case ErrCodeOutOfDiskSpace:
		return "OUT_OF_DISK_SPACE"
	case ErrCodeOutOfCapacity:
		return "OUT_OF_CAPACITY"

	default:
		return "UNKNOWN"
	}
}

func (e ErrorCode) IsRetryable() bool {
	switch e {
	case ErrCodeDatabaseConnectionFailed,
		ErrCodeDatabaseQueryFailed,
		ErrCodeRedisConnectionFailed,
		ErrCodeRedisOperationFailed,
		ErrCodeNetworkError,
		ErrCodeConnectionRefused,
		ErrCodeConnectionTimeout,
		ErrCodeRequestTimeout,
		ErrCodeResourceBusy,
		ErrCodeServiceUnavailable:
		return true
	default:
		return false
	}
}

func (e ErrorCode) IsFatal() bool {
	switch e {
	case ErrCodeUnauthorized,
		ErrCodeForbidden,
		ErrCodeInvalidParameter,
		ErrCodeMissingParameter,
		ErrCodeInvalidConfiguration,
		ErrCodeValidationFailed,
		ErrCodeSerializationFailed,
		ErrCodeDeserializationFailed,
		ErrCodeFilePermissionDenied:
		return true
	default:
		return false
	}
}

type Error struct {
	Code      ErrorCode `json:"code"`
	Message   string    `json:"message"`
	Details   string    `json:"details,omitempty"`
	Err       error      `json:"-"`
	Stack     string    `json:"stack,omitempty"`
	TaskID    string    `json:"task_id,omitempty"`
	PipelineID string   `json:"pipeline_id,omitempty"`
	NodeID    string    `json:"node_id,omitempty"`
}

func (e *Error) Error() string {
	if e == nil {
		return ""
	}

	parts := []string{fmt.Sprintf("[%s] %s", e.Code.String(), e.Message)}
	if e.Details != "" {
		parts = append(parts, fmt.Sprintf("details: %s", e.Details))
	}
	if e.TaskID != "" {
		parts = append(parts, fmt.Sprintf("task_id: %s", e.TaskID))
	}
	if e.PipelineID != "" {
		parts = append(parts, fmt.Sprintf("pipeline_id: %s", e.PipelineID))
	}
	if e.NodeID != "" {
		parts = append(parts, fmt.Sprintf("node_id: %s", e.NodeID))
	}
	if e.Err != nil {
		parts = append(parts, fmt.Sprintf("cause: %v", e.Err))
	}

	return strings.Join(parts, " | ")
}

func (e *Error) Unwrap() error {
	return e.Err
}

func (e *Error) WithTaskID(taskID string) *Error {
	e.TaskID = taskID
	return e
}

func (e *Error) WithPipelineID(pipelineID string) *Error {
	e.PipelineID = pipelineID
	return e
}

func (e *Error) WithNodeID(nodeID string) *Error {
	e.NodeID = nodeID
	return e
}

func (e *Error) WithDetails(details string) *Error {
	e.Details = details
	return e
}

func (e *Error) WithStack(stack string) *Error {
	e.Stack = stack
	return e
}

func (e *Error) WithCause(err error) *Error {
	e.Err = err
	return e
}

func New(code ErrorCode, message string) *Error {
	return &Error{
		Code:    code,
		Message: message,
	}
}

func Newf(code ErrorCode, format string, args ...interface{}) *Error {
	return &Error{
		Code:    code,
		Message: fmt.Sprintf(format, args...),
	}
}

func NewInternalf(format string, args ...interface{}) *Error {
	return &Error{
		Code:    ErrCodeInternalServerError,
		Message: fmt.Sprintf(format, args...),
	}
}

func Wrap(err error, code ErrorCode, message string) *Error {
	if err == nil {
		return nil
	}
	return &Error{
		Code:    code,
		Message: message,
		Err:     err,
	}
}

func Wrapf(err error, code ErrorCode, format string, args ...interface{}) *Error {
	if err == nil {
		return nil
	}
	return &Error{
		Code:    code,
		Message: fmt.Sprintf(format, args...),
		Err:     err,
	}
}

func WrapIfNotNil(err error, code ErrorCode, message string) *Error {
	if err == nil {
		return nil
	}
	return &Error{
		Code:    code,
		Message: message,
		Err:     err,
	}
}

func WithTaskID(err error, taskID string) *Error {
	if err == nil {
		return nil
	}
	if e, ok := err.(*Error); ok {
		return e.WithTaskID(taskID)
	}
	return &Error{
		Code:    ErrCodeUnknown,
		Message: err.Error(),
		Err:     err,
		TaskID:  taskID,
	}
}

func WithPipelineID(err error, pipelineID string) *Error {
	if err == nil {
		return nil
	}
	if e, ok := err.(*Error); ok {
		return e.WithPipelineID(pipelineID)
	}
	return &Error{
		Code:       ErrCodeUnknown,
		Message:    err.Error(),
		Err:        err,
		PipelineID: pipelineID,
	}
}

func WithNodeID(err error, nodeID string) *Error {
	if err == nil {
		return nil
	}
	if e, ok := err.(*Error); ok {
		return e.WithNodeID(nodeID)
	}
	return &Error{
		Code:    ErrCodeUnknown,
		Message: err.Error(),
		Err:     err,
		NodeID:  nodeID,
	}
}

func IsError(err error) bool {
	_, ok := err.(*Error)
	return ok
}

func AsError(err error) (*Error, bool) {
	e, ok := err.(*Error)
	return e, ok
}

func GetErrorCode(err error) ErrorCode {
	if err == nil {
		return 0
	}
	if e, ok := err.(*Error); ok {
		return e.Code
	}
	return ErrCodeUnknown
}

func GetErrorMessage(err error) string {
	if err == nil {
		return ""
	}
	if e, ok := err.(*Error); ok {
		return e.Message
	}
	return err.Error()
}

func NewPipelineNotFound(pipelineID string) *Error {
	return &Error{
		Code:       ErrCodePipelineNotFound,
		Message:    fmt.Sprintf("pipeline not found: %s", pipelineID),
		PipelineID: pipelineID,
	}
}

func NewNotFoundf(format string, args ...interface{}) *Error {
	return &Error{
		Code:    ErrCodeNotFound,
		Message: fmt.Sprintf(format, args...),
	}
}

func NewInvalidRequestf(format string, args ...interface{}) *Error {
	return &Error{
		Code:    ErrCodeInvalidParameter,
		Message: fmt.Sprintf(format, args...),
	}
}

func NewPipelineAlreadyExists(pipelineID string) *Error {
	return &Error{
		Code:       ErrCodePipelineAlreadyExists,
		Message:    fmt.Sprintf("pipeline already exists: %s", pipelineID),
		PipelineID: pipelineID,
	}
}

func NewPipelineFailed(pipelineID string, reason string) *Error {
	return &Error{
		Code:       ErrCodePipelineFailed,
		Message:    fmt.Sprintf("pipeline failed: %s", reason),
		PipelineID: pipelineID,
	}
}

func NewNodeNotFound(nodeID string) *Error {
	return &Error{
		Code:   ErrCodeNodeNotFound,
		Message: fmt.Sprintf("node not found: %s", nodeID),
		NodeID: nodeID,
	}
}

func NewNodeFailed(nodeID string, reason string) *Error {
	return &Error{
		Code:   ErrCodeNodeFailed,
		Message: fmt.Sprintf("node failed: %s", reason),
		NodeID: nodeID,
	}
}

func NewTaskNotFound(taskID string) *Error {
	return &Error{
		Code:   ErrCodeTaskNotFound,
		Message: fmt.Sprintf("task not found: %s", taskID),
		TaskID: taskID,
	}
}

func NewTaskFailed(taskID string, reason string) *Error {
	return &Error{
		Code:   ErrCodeTaskFailed,
		Message: fmt.Sprintf("task failed: %s", reason),
		TaskID: taskID,
	}
}

func NewTaskTimeout(taskID string) *Error {
	return &Error{
		Code:   ErrCodeTaskTimeout,
		Message: fmt.Sprintf("task timeout: %s", taskID),
		TaskID: taskID,
	}
}

func NewTaskRetryExhausted(taskID string) *Error {
	return &Error{
		Code:   ErrCodeTaskRetryExhausted,
		Message: fmt.Sprintf("task retry exhausted: %s", taskID),
		TaskID: taskID,
	}
}

func NewWorkerNotFound(workerID string) *Error {
	return &Error{
		Code:    ErrCodeWorkerNotFound,
		Message: fmt.Sprintf("worker not found: %s", workerID),
	}
}

func NewWorkerHeartbeatTimeout(workerID string) *Error {
	return &Error{
		Code:    ErrCodeWorkerHeartbeatTimeout,
		Message: fmt.Sprintf("worker heartbeat timeout: %s", workerID),
	}
}

func NewDatabaseError(reason string) *Error {
	return &Error{
		Code:    ErrCodeDatabaseError,
		Message: fmt.Sprintf("database error: %s", reason),
	}
}

func NewRedisError(reason string) *Error {
	return &Error{
		Code:    ErrCodeRedisError,
		Message: fmt.Sprintf("redis error: %s", reason),
	}
}

func NewInvalidParameter(name string, reason string) *Error {
	return &Error{
		Code:    ErrCodeInvalidParameter,
		Message: fmt.Sprintf("invalid parameter '%s': %s", name, reason),
	}
}

func NewMissingParameter(name string) *Error {
	return &Error{
		Code:    ErrCodeMissingParameter,
		Message: fmt.Sprintf("missing required parameter: %s", name),
	}
}

func NewInvalidConfiguration(reason string) *Error {
	return &Error{
		Code:    ErrCodeInvalidConfiguration,
		Message: fmt.Sprintf("invalid configuration: %s", reason),
	}
}

func NewConfigurationNotFound(key string) *Error {
	return &Error{
		Code:    ErrCodeConfigurationNotFound,
		Message: fmt.Sprintf("configuration not found: %s", key),
	}
}

func NewUnauthorized(reason string) *Error {
	return &Error{
		Code:    ErrCodeUnauthorized,
		Message: fmt.Sprintf("unauthorized: %s", reason),
	}
}

func NewForbidden(reason string) *Error {
	return &Error{
		Code:    ErrCodeForbidden,
		Message: fmt.Sprintf("forbidden: %s", reason),
	}
}

func NewValidationFailed(reason string) *Error {
	return &Error{
		Code:    ErrCodeValidationFailed,
		Message: fmt.Sprintf("validation failed: %s", reason),
	}
}

type ErrorGroup struct {
	errors []*Error
}

func NewErrorGroup() *ErrorGroup {
	return &ErrorGroup{
		errors: make([]*Error, 0),
	}
}

func (g *ErrorGroup) Add(err *Error) {
	if err != nil {
		g.errors = append(g.errors, err)
	}
}

func (g *ErrorGroup) AddIfNotNil(err error) {
	if err != nil {
		if e, ok := err.(*Error); ok {
			g.errors = append(g.errors, e)
		} else {
			g.errors = append(g.errors, &Error{
				Code:    ErrCodeUnknown,
				Message: err.Error(),
				Err:     err,
			})
		}
	}
}

func (g *ErrorGroup) Errors() []*Error {
	return g.errors
}

func (g *ErrorGroup) Error() string {
	if len(g.errors) == 0 {
		return ""
	}
	if len(g.errors) == 1 {
		return g.errors[0].Error()
	}
	return g.String()
}

func (g *ErrorGroup) String() string {
	if len(g.errors) == 0 {
		return ""
	}
	var parts []string
	for _, err := range g.errors {
		parts = append(parts, err.Error())
	}
	return strings.Join(parts, "\n")
}

func (g *ErrorGroup) IsEmpty() bool {
	return len(g.errors) == 0
}

func (g *ErrorGroup) Size() int {
	return len(g.errors)
}

func (g *ErrorGroup) HasError() bool {
	return len(g.errors) > 0
}

func (g *ErrorGroup) HasCode(code ErrorCode) bool {
	for _, err := range g.errors {
		if err.Code == code {
			return true
		}
	}
	return false
}

func (g *ErrorGroup) FirstError() *Error {
	if len(g.errors) > 0 {
		return g.errors[0]
	}
	return nil
}

func (g *ErrorGroup) FilterByCode(code ErrorCode) []*Error {
	result := make([]*Error, 0)
	for _, err := range g.errors {
		if err.Code == code {
			result = append(result, err)
		}
	}
	return result
}
