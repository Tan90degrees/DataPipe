from enum import Enum
from typing import Any, Dict, Optional
from dataclasses import dataclass
import base64


class DataType(Enum):
    ANY = "any"
    STRING = "string"
    INTEGER = "integer"
    FLOAT = "float"
    BOOLEAN = "boolean"
    ARRAY = "array"
    OBJECT = "object"
    FILE = "file"
    BINARY = "binary"

    @classmethod
    def from_string(cls, value: str) -> "DataType":
        for member in cls:
            if member.value == value.lower():
                return member
        return cls.ANY

    def is_compatible_with(self, other: "DataType") -> bool:
        if self == cls.ANY or other == cls.ANY:
            return True
        return self == other


@dataclass
class FileContent:
    data: bytes = b""
    encoding: str = "utf-8"

    def __post_init__(self):
        if isinstance(self.data, str):
            self.data = self.data.encode(self.encoding)

    @property
    def text(self) -> str:
        return self.data.decode(self.encoding)

    @property
    def base64(self) -> str:
        return base64.b64encode(self.data).decode("ascii")

    def to_dict(self) -> Dict[str, Any]:
        return {
            "data": self.base64,
            "encoding": self.encoding,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "FileContent":
        encoded_data = data.get("data", "")
        if isinstance(encoded_data, str):
            encoded_data = base64.b64decode(encoded_data)
        return cls(
            data=encoded_data,
            encoding=data.get("encoding", "utf-8"),
        )


@dataclass
class FileInfo:
    filename: str = ""
    content_type: str = "application/octet-stream"
    size: int = 0
    path: str = ""
    checksum: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "filename": self.filename,
            "content_type": self.content_type,
            "size": self.size,
            "path": self.path,
            "checksum": self.checksum,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "FileInfo":
        return cls(
            filename=data.get("filename", ""),
            content_type=data.get("content_type", "application/octet-stream"),
            size=data.get("size", 0),
            path=data.get("path", ""),
            checksum=data.get("checksum"),
        )
