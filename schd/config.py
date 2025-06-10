from dataclasses import dataclass, field, fields, is_dataclass
import os
from typing import Any, Dict, Optional, Type, TypeVar
import yaml

T = TypeVar("T", bound="ConfigValue")


class ConfigValue:
    """
    ConfigValue present some config settings.
    A configvalue class should also be decorated as @dataclass.
    A ConfigValue class contains some fields, for example:

    @dataclass
    class SimpleIntValue(ConfigValue):
        a: int

    User can call derived class 's from_dict class method to construct an instance.
    config = SimpleIntValue.from_dict({'a': 1})
    """
    @classmethod
    def from_dict(cls: Type[T], data: Dict[str, Any]) -> T:
        """
        Creates an instance of the class using the fields specified in the dictionary.
        Handles nested fields that are also derived from ConfigValue.
        """
        if not is_dataclass(cls):
            raise TypeError(f"{cls} is not a dataclass")

        field_names = {f.name: f.type for f in fields(cls)}
        init_data = {}
        for key, field_type in field_names.items():
            if key in data:
                value = data[key]
                # If the field type is a subclass of ConfigValue, parse recursively
                if isinstance(field_type, type) and issubclass(field_type, ConfigValue):
                    init_data[key] = field_type.from_dict(value)
                else:
                    try:
                        init_data[key] = field_type(value)
                    except ValueError as ex:
                        raise TypeError(f'error when converting value {value} into type {field_type}') from ex

        return cls(**init_data)


@dataclass
class JobConfig(ConfigValue):
    cls: str = field(metadata={"json": "class"})
    cron: str
    cmd: Optional[str] = None


@dataclass
class SchdConfig(ConfigValue):
    jobs: Dict[str, JobConfig] = field(default=dict)
    scheduler_cls: str = "LocalScheduler"
    worker_name: str = 'local'

    def __getitem__(self,key):
        if hasattr(self, key):
            return getattr(self,key)
        else:
            raise KeyError(key)


def read_config(config_file=None) -> SchdConfig:
    if config_file is None and 'SCHD_CONFIG' in os.environ:
        config_file = os.environ['SCHD_CONFIG']

    if config_file is None:
        config_file = 'conf/schd.yaml'

    with open(config_file, 'r', encoding='utf8') as f:
        config = SchdConfig(**yaml.load(f, Loader=yaml.FullLoader))
        return config
