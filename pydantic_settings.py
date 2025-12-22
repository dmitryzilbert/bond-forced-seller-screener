from __future__ import annotations

import os
from typing import Any

from dotenv import load_dotenv


class SettingsConfigDict(dict):
    pass

class BaseSettings:
    def __init__(self, **kwargs: Any):
        model_config = getattr(self.__class__, "model_config", SettingsConfigDict())
        env_file = model_config.get("env_file") if isinstance(model_config, dict) else None
        env_file_encoding = model_config.get("env_file_encoding") if isinstance(model_config, dict) else None
        case_sensitive = model_config.get("case_sensitive", False) if isinstance(model_config, dict) else False

        if env_file:
            load_dotenv(env_file, override=False, encoding=env_file_encoding)

        for field in dir(self.__class__):
            if field.startswith('_'):
                continue
            attr = getattr(self.__class__, field)
            default = attr.default if hasattr(attr, 'default') else attr
            alias = attr.alias if hasattr(attr, 'alias') else field.upper()
            value = kwargs.get(field)

            if value is None:
                if case_sensitive:
                    value = os.getenv(alias)
                    if value is None:
                        value = os.getenv(field)
                else:
                    lower_alias = alias.lower()
                    lower_field = field.lower()
                    for key, env_value in os.environ.items():
                        if key.lower() in {lower_alias, lower_field}:
                            value = env_value
                            break
                if value is None:
                    value = default
            setattr(self, field, value)

    class Config:
        env_file = None
        case_sensitive = False
