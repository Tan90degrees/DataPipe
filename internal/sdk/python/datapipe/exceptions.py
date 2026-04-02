class DataPipeError(Exception):
    def __init__(self, message: str, code: str = "DATAPIPE_ERROR"):
        self.message = message
        self.code = code
        super().__init__(self.message)


class FunctionError(DataPipeError):
    def __init__(self, message: str, function_name: str = ""):
        self.function_name = function_name
        super().__init__(message, code="FUNCTION_ERROR")


class ExecutionError(DataPipeError):
    def __init__(self, message: str, execution_id: str = ""):
        self.execution_id = execution_id
        super().__init__(message, code="EXECUTION_ERROR")


class ConfigurationError(DataPipeError):
    def __init__(self, message: str, config_key: str = ""):
        self.config_key = config_key
        super().__init__(message, code="CONFIGURATION_ERROR")


class ValidationError(DataPipeError):
    def __init__(self, message: str, field: str = ""):
        self.field = field
        super().__init__(message, code="VALIDATION_ERROR")


class TimeoutError(DataPipeError):
    def __init__(self, message: str, timeout: int = 0):
        self.timeout = timeout
        super().__init__(message, code="TIMEOUT_ERROR")


class NotFoundError(DataPipeError):
    def __init__(self, message: str, resource_type: str = ""):
        self.resource_type = resource_type
        super().__init__(message, code="NOT_FOUND_ERROR")
