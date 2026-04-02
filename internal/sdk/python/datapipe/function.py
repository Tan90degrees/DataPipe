from typing import Any, Callable, Dict, List, Optional, Type
from dataclasses import dataclass, field
from enum import Enum


class FunctionRegistry:
    _functions: Dict[str, Type["Function"]] = {}

    @classmethod
    def register(cls, name: str, func_class: Type["Function"]) -> None:
        cls._functions[name] = func_class

    @classmethod
    def get(cls, name: str) -> Optional[Type["Function"]]:
        return cls._functions.get(name)

    @classmethod
    def list_functions(cls) -> List[str]:
        return list(cls._functions.keys())


class Input:
    def __init__(
        self,
        name: str,
        data_type: str = "any",
        required: bool = True,
        default: Any = None,
    ):
        self.name = name
        self.data_type = data_type
        self.required = required
        self.default = default

    def to_dict(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "data_type": self.data_type,
            "required": self.required,
            "default": self.default,
        }


class Output:
    def __init__(
        self,
        name: str,
        data_type: str = "any",
        description: str = "",
    ):
        self.name = name
        self.data_type = data_type
        self.description = description

    def to_dict(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "data_type": self.data_type,
            "description": self.description,
        }


@dataclass
class FunctionConfig:
    name: str = ""
    description: str = ""
    version: str = "0.1.0"
    inputs: List[Input] = field(default_factory=list)
    outputs: List[Output] = field(default_factory=list)
    parameters: Dict[str, Any] = field(default_factory=dict)
    timeout: int = 300
    retry_count: int = 0

    def to_dict(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "description": self.description,
            "version": self.version,
            "inputs": [inp.to_dict() for inp in self.inputs],
            "outputs": [out.to_dict() for out in self.outputs],
            "parameters": self.parameters,
            "timeout": self.timeout,
            "retry_count": self.retry_count,
        }


class Function:
    _config: FunctionConfig = FunctionConfig()
    _registry: FunctionRegistry = FunctionRegistry()

    def __init__(self, config: Optional[FunctionConfig] = None):
        if config is not None:
            self._config = config

    @property
    def config(self) -> FunctionConfig:
        return self._config

    @property
    def name(self) -> str:
        return self._config.name

    @property
    def inputs(self) -> List[Input]:
        return self._config.inputs

    @property
    def outputs(self) -> List[Output]:
        return self._config.outputs

    def execute(self, inputs: Dict[str, Any], context: Any) -> Dict[str, Any]:
        raise NotImplementedError("Subclasses must implement execute method")

    @classmethod
    def register(cls, name: Optional[str] = None) -> Callable:
        def decorator(func_class: Type[Function]) -> Type[Function]:
            registered_name = name or func_class.__name__
            func_class._config.name = registered_name
            cls._registry.register(registered_name, func_class)
            return func_class
        return decorator

    @classmethod
    def get_registered(cls, name: str) -> Optional[Type[Function]]:
        return cls._registry.get(name)

    @classmethod
    def list_registered(cls) -> List[str]:
        return cls._registry.list_functions()

    def validate_inputs(self, inputs: Dict[str, Any]) -> None:
        for inp in self.inputs:
            if inp.required and inp.name not in inputs:
                if inp.default is None:
                    raise ValueError(f"Required input '{inp.name}' is missing")
