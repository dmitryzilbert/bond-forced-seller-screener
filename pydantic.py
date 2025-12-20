from __future__ import annotations

from dataclasses import dataclass, field
import os
import sys
import types
from typing import Any

version_module = types.ModuleType("pydantic.version")
version_module.VERSION = "0.0.0"
sys.modules["pydantic.version"] = version_module


@dataclass
class _FieldInfo:
    default: Any
    alias: str | None = None


def Field(default: Any = None, alias: str | None = None, default_factory=None, **kwargs):
    if default is None and default_factory is not None:
        default = default_factory()
    return _FieldInfo(default=default, alias=alias)


def model_validator(*args, **kwargs):
    def decorator(func):
        return func

    return decorator


def PlainSerializer(*args, **kwargs):
    def decorator(func):
        return func

    return decorator


class BaseModel:
    def __init__(self, **data: Any):
        for k, v in data.items():
            setattr(self, k, v)

    def __iter__(self):
        return iter(self.__dict__.items())

    def model_dump(self) -> dict:
        return self.__dict__

    def dict(self):
        return self.model_dump()

    @classmethod
    def model_rebuild(cls, *args, **kwargs):
        return None


class BaseConfig:
    pass


class ValidationError(Exception):
    pass


class ConfigDict(dict):
    pass


class PydanticSchemaGenerationError(Exception):
    pass


class PydanticUndefinedAnnotation(Exception):
    pass


class TypeAdapter:
    def __init__(self, type_):
        self.type_ = type_

    def validate_python(self, value):
        return value


def create_model(name: str, **fields: Any):
    return type(name, (BaseModel,), fields)


class Validator:
    pass


class Color(str):
    pass


class ErrorWrapper(Exception):
    pass


SHAPE_SINGLETON = 0
SHAPE_LIST = 1
SHAPE_SET = 2
SHAPE_FROZENSET = 3
SHAPE_TUPLE = 4
SHAPE_SEQUENCE = 5
SHAPE_TUPLE_ELLIPSIS = 6


class FieldInfo:
    def __init__(self, default: Any = None, alias: str | None = None, **kwargs):
        self.default = default
        self.alias = alias


class ModelField:
    def __init__(self, name: str = "", type_: Any = Any, default: Any = None, **kwargs):
        self.name = name or kwargs.get("name", "")
        self.type_ = type_ or kwargs.get("type_", Any)
        self.field_info = kwargs.get("field_info", FieldInfo(default))
        self.shape = kwargs.get("shape", SHAPE_SINGLETON)
        self.sub_fields = kwargs.get("sub_fields", [])
        self.alias = kwargs.get("alias", self.name)
        self.validation_alias = kwargs.get("validation_alias")
        self.required = kwargs.get("required", False)
        self.default = kwargs.get("default", default)

    def validate(self, value, values, loc=None):
        return value, []


Undefined = object()


class UndefinedType:
    pass


class AnyUrl(str):
    pass


class NameEmail(str):
    pass


def field_schema(*args, **kwargs):
    return {}, {}, []


def model_process_schema(*args, **kwargs):
    return {}, {}, []


def get_annotation_from_field_info(*args, **kwargs):
    return None


def get_flat_models_from_field(*args, **kwargs):
    return set()


def get_flat_models_from_fields(*args, **kwargs):
    return set()


def get_model_name_map(models):
    return {}


def lenient_issubclass(cls, class_or_tuple) -> bool:
    try:
        return issubclass(cls, class_or_tuple)
    except Exception:
        return False


def evaluate_forwardref(type_, globalns=None, localns=None):
    return type_


class SecretBytes(bytes):
    pass


class SecretStr(str):
    pass


class CoreSchema(dict):
    pass


class PrivateAttr:
    def __init__(self, default=None):
        self.default = default


class PydanticUndefinedType:
    pass


PydanticUndefined = PydanticUndefinedType()
Url = str


v1_module = types.ModuleType("pydantic.v1")
v1_module.BaseConfig = BaseConfig
v1_module.BaseModel = BaseModel
v1_module.ValidationError = ValidationError
v1_module.create_model = create_model
v1_module.SHAPE_SINGLETON = SHAPE_SINGLETON
v1_module.SHAPE_LIST = SHAPE_LIST
v1_module.SHAPE_SET = SHAPE_SET
v1_module.SHAPE_FROZENSET = SHAPE_FROZENSET
v1_module.SHAPE_TUPLE = SHAPE_TUPLE
v1_module.SHAPE_SEQUENCE = SHAPE_SEQUENCE
v1_module.SHAPE_TUPLE_ELLIPSIS = SHAPE_TUPLE_ELLIPSIS
v1_module.FieldInfo = FieldInfo
v1_module.ModelField = ModelField
v1_module.Undefined = Undefined
v1_module.UndefinedType = UndefinedType
v1_module.AnyUrl = AnyUrl
v1_module.NameEmail = NameEmail
v1_module.SecretBytes = SecretBytes
v1_module.SecretStr = SecretStr
v1_module.evaluate_forwardref = evaluate_forwardref
v1_module.lenient_issubclass = lenient_issubclass
v1_module.field_schema = field_schema
v1_module.model_process_schema = model_process_schema
v1_module.get_annotation_from_field_info = get_annotation_from_field_info
v1_module.get_flat_models_from_field = get_flat_models_from_field
v1_module.get_flat_models_from_fields = get_flat_models_from_fields
v1_module.get_model_name_map = get_model_name_map

v1_class_validators = types.ModuleType("pydantic.v1.class_validators")
v1_class_validators.Validator = Validator

v1_color = types.ModuleType("pydantic.v1.color")
v1_color.Color = Color

v1_error_wrappers = types.ModuleType("pydantic.v1.error_wrappers")
v1_error_wrappers.ErrorWrapper = ErrorWrapper

v1_fields = types.ModuleType("pydantic.v1.fields")
v1_fields.SHAPE_FROZENSET = SHAPE_FROZENSET
v1_fields.SHAPE_LIST = SHAPE_LIST
v1_fields.SHAPE_SEQUENCE = SHAPE_SEQUENCE
v1_fields.SHAPE_SET = SHAPE_SET
v1_fields.SHAPE_SINGLETON = SHAPE_SINGLETON
v1_fields.SHAPE_TUPLE = SHAPE_TUPLE
v1_fields.SHAPE_TUPLE_ELLIPSIS = SHAPE_TUPLE_ELLIPSIS
v1_fields.FieldInfo = FieldInfo
v1_fields.ModelField = ModelField
v1_fields.Undefined = Undefined
v1_fields.UndefinedType = UndefinedType

v1_networks = types.ModuleType("pydantic.v1.networks")
v1_networks.AnyUrl = AnyUrl
v1_networks.NameEmail = NameEmail

v1_schema = types.ModuleType("pydantic.v1.schema")
v1_schema.TypeModelSet = set
v1_schema.field_schema = field_schema
v1_schema.model_process_schema = model_process_schema
v1_schema.get_annotation_from_field_info = get_annotation_from_field_info
v1_schema.get_flat_models_from_field = get_flat_models_from_field
v1_schema.get_flat_models_from_fields = get_flat_models_from_fields
v1_schema.get_model_name_map = get_model_name_map

v1_types = types.ModuleType("pydantic.v1.types")
v1_types.SecretBytes = SecretBytes
v1_types.SecretStr = SecretStr

v1_typing = types.ModuleType("pydantic.v1.typing")
v1_typing.evaluate_forwardref = evaluate_forwardref

v1_utils = types.ModuleType("pydantic.v1.utils")
v1_utils.lenient_issubclass = lenient_issubclass

v1_module.class_validators = v1_class_validators
v1_module.color = v1_color
v1_module.error_wrappers = v1_error_wrappers
v1_module.fields = v1_fields
v1_module.networks = v1_networks
v1_module.schema = v1_schema
v1_module.types = v1_types
v1_module.typing = v1_typing
v1_module.utils = v1_utils

sys.modules["pydantic.v1"] = v1_module
sys.modules["pydantic.v1.class_validators"] = v1_class_validators
sys.modules["pydantic.v1.color"] = v1_color
sys.modules["pydantic.v1.error_wrappers"] = v1_error_wrappers
sys.modules["pydantic.v1.fields"] = v1_fields
sys.modules["pydantic.v1.networks"] = v1_networks
sys.modules["pydantic.v1.schema"] = v1_schema
sys.modules["pydantic.v1.types"] = v1_types
sys.modules["pydantic.v1.typing"] = v1_typing
sys.modules["pydantic.v1.utils"] = v1_utils

# pydantic v2 style placeholders for FastAPI compat
fields_module = types.ModuleType("pydantic.fields")
fields_module.FieldInfo = FieldInfo
sys.modules["pydantic.fields"] = fields_module

json_schema_module = types.ModuleType("pydantic.json_schema")
json_schema_module.GenerateJsonSchema = type("GenerateJsonSchema", (), {})
json_schema_module.JsonSchemaValue = dict
sys.modules["pydantic.json_schema"] = json_schema_module

internal_schema_shared = types.ModuleType("pydantic._internal._schema_generation_shared")
internal_schema_shared.GetJsonSchemaHandler = Any
sys.modules["pydantic._internal._schema_generation_shared"] = internal_schema_shared

internal_typing_extra = types.ModuleType("pydantic._internal._typing_extra")
internal_typing_extra.eval_type_lenient = evaluate_forwardref
sys.modules["pydantic._internal._typing_extra"] = internal_typing_extra

internal_utils = types.ModuleType("pydantic._internal._utils")
internal_utils.lenient_issubclass = lenient_issubclass
sys.modules["pydantic._internal._utils"] = internal_utils

pydantic_core_module = types.ModuleType("pydantic_core")
pydantic_core_module.CoreSchema = CoreSchema
pydantic_core_module.PydanticUndefined = PydanticUndefined
pydantic_core_module.PydanticUndefinedType = PydanticUndefinedType
pydantic_core_module.Url = Url
sys.modules["pydantic_core"] = pydantic_core_module

pydantic_core_schema = types.ModuleType("pydantic_core.core_schema")

def with_info_plain_validator_function(*args, **kwargs):
    return {}

pydantic_core_schema.with_info_plain_validator_function = with_info_plain_validator_function
sys.modules["pydantic_core.core_schema"] = pydantic_core_schema

color_module = types.ModuleType("pydantic.color")
color_module.Color = Color
sys.modules["pydantic.color"] = color_module

networks_module = types.ModuleType("pydantic.networks")
networks_module.AnyUrl = AnyUrl
networks_module.NameEmail = NameEmail
sys.modules["pydantic.networks"] = networks_module

types_module = types.ModuleType("pydantic.types")
types_module.SecretBytes = SecretBytes
types_module.SecretStr = SecretStr
sys.modules["pydantic.types"] = types_module

functional_validators_module = types.ModuleType("pydantic.functional_validators")
functional_validators_module.model_validator = model_validator
sys.modules["pydantic.functional_validators"] = functional_validators_module
