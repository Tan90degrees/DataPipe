from typing import Any, Dict, Optional
from dataclasses import dataclass, field
from datetime import datetime


@dataclass
class DataRecord:
    id: str = ""
    name: str = ""
    data_type: str = "any"
    value: Any = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    def __post_init__(self):
        if self.created_at is None:
            self.created_at = datetime.now()
        if self.updated_at is None:
            self.updated_at = datetime.now()

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "name": self.name,
            "data_type": self.data_type,
            "value": self.value,
            "metadata": self.metadata,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "DataRecord":
        created_at = data.get("created_at")
        if created_at and isinstance(created_at, str):
            created_at = datetime.fromisoformat(created_at)
        updated_at = data.get("updated_at")
        if updated_at and isinstance(updated_at, str):
            updated_at = datetime.fromisoformat(updated_at)
        return cls(
            id=data.get("id", ""),
            name=data.get("name", ""),
            data_type=data.get("data_type", "any"),
            value=data.get("value"),
            metadata=data.get("metadata", {}),
            created_at=created_at,
            updated_at=updated_at,
        )


@dataclass
class ExecutionContext:
    execution_id: str = ""
    pipeline_id: str = ""
    node_id: str = ""
    parameters: Dict[str, Any] = field(default_factory=dict)
    inputs: Dict[str, DataRecord] = field(default_factory=dict)
    outputs: Dict[str, DataRecord] = field(default_factory=dict)
    metadata: Dict[str, Any] = field(default_factory=dict)
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None

    def __post_init__(self):
        if self.start_time is None:
            self.start_time = datetime.now()

    def get_input(self, name: str) -> Optional[DataRecord]:
        return self.inputs.get(name)

    def set_output(self, name: str, record: DataRecord) -> None:
        self.outputs[name] = record

    def get_output(self, name: str) -> Optional[DataRecord]:
        return self.outputs.get(name)

    def mark_complete(self) -> None:
        self.end_time = datetime.now()

    def duration(self) -> Optional[float]:
        if self.start_time and self.end_time:
            return (self.end_time - self.start_time).total_seconds()
        return None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "execution_id": self.execution_id,
            "pipeline_id": self.pipeline_id,
            "node_id": self.node_id,
            "parameters": self.parameters,
            "inputs": {k: v.to_dict() for k, v in self.inputs.items()},
            "outputs": {k: v.to_dict() for k, v in self.outputs.items()},
            "metadata": self.metadata,
            "start_time": self.start_time.isoformat() if self.start_time else None,
            "end_time": self.end_time.isoformat() if self.end_time else None,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "ExecutionContext":
        start_time = data.get("start_time")
        if start_time and isinstance(start_time, str):
            start_time = datetime.fromisoformat(start_time)
        end_time = data.get("end_time")
        if end_time and isinstance(end_time, str):
            end_time = datetime.fromisoformat(end_time)
        inputs = {
            k: DataRecord.from_dict(v) if isinstance(v, dict) else v
            for k, v in data.get("inputs", {}).items()
        }
        outputs = {
            k: DataRecord.from_dict(v) if isinstance(v, dict) else v
            for k, v in data.get("outputs", {}).items()
        }
        return cls(
            execution_id=data.get("execution_id", ""),
            pipeline_id=data.get("pipeline_id", ""),
            node_id=data.get("node_id", ""),
            parameters=data.get("parameters", {}),
            inputs=inputs,
            outputs=outputs,
            metadata=data.get("metadata", {}),
            start_time=start_time,
            end_time=end_time,
        )
