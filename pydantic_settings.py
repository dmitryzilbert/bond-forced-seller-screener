from __future__ import annotations

import os
from typing import Any

class BaseSettings:
    def __init__(self, **kwargs: Any):
        for field in dir(self.__class__):
            if field.startswith('_'):
                continue
            attr = getattr(self.__class__, field)
            default = attr.default if hasattr(attr, 'default') else attr
            alias = attr.alias if hasattr(attr, 'alias') else field.upper()
            value = kwargs.get(field, os.getenv(alias, default))
            setattr(self, field, value)

    class Config:
        env_file = None
        case_sensitive = False
