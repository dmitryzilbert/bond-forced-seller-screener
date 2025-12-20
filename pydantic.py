from __future__ import annotations

from dataclasses import dataclass, field
import os
from typing import Any


@dataclass
class _FieldInfo:
    default: Any
    alias: str | None = None


def Field(default: Any = None, alias: str | None = None, default_factory=None):
    if default is None and default_factory is not None:
        default = default_factory()
    return _FieldInfo(default=default, alias=alias)


class BaseModel:
    def __init__(self, **data: Any):
        for k, v in data.items():
            setattr(self, k, v)

    def model_dump(self) -> dict:
        return self.__dict__

    def __iter__(self):
        return iter(self.__dict__.items())

    def dict(self):
        return self.model_dump()
