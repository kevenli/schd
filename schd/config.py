from dataclasses import dataclass, field, fields, is_dataclass
import os
from typing import Any, Dict, Optional, Type, TypeVar, Union, get_args, get_origin, get_type_hints
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
        type_hints = get_type_hints(cls)
        init_data:Dict[str,Any] = {}
        if not is_dataclass(cls):
            raise TypeError('class %s is not dataclass' % cls)
        
        for f in fields(cls):
            field_name = f.name
            json_key = f.metadata.get("json", f.name)
            envvar_key = f.metadata.get('env_var')
            field_type = type_hints[field_name]
            origin = get_origin(field_type)
            args = get_args(field_type)

            if envvar_key and envvar_key in os.environ:
                init_data[field_name] = _cast_type(os.environ[envvar_key], field_type)
                continue

            if json_key in data:
                value = data[json_key]
                # Handle nested ConfigValue objects
                if isinstance(field_type, type) and issubclass(field_type, ConfigValue):
                    init_data[field_name] = field_type.from_dict(value)
                # Handle lists of ConfigValue objects   List[ConfigValue]
                elif origin is list and issubclass(args[0], ConfigValue):
                    nested_type = field_type.__args__[0]
                    init_data[field_name] = [nested_type.from_dict(item) for item in value]
                    # Handle Optional[ConfigValue]
                elif origin is Union and type(None) in args:
                    actual_type = next((arg for arg in args if arg is not type(None)), None)
                    if actual_type and issubclass(actual_type, ConfigValue):
                        init_data[field_name] = actual_type.from_dict(value) if value is not None else None
                    else:
                        init_data[field_name] = value
                # Case 4: Dict[str, ConfigValue]
                elif origin is dict and issubclass(args[1], ConfigValue):
                    value_type = args[1]
                    init_data[field_name] = {
                        k: value_type.from_dict(v) for k, v in value.items()
                    }
                else:
                    init_data[field_name] = value
        return cls(**init_data)

def _cast_type(value, target_type):
    origin = get_origin(target_type)
    args = get_args(target_type)

    # Handle Optional[T] or Union[T1, T2, ...]
    if origin is Union:
        # Optional[str] is Union[str, NoneType]
        for typ in args:
            if typ is type(None):
                continue  # skip NoneType
            try:
                return _cast_type(value, typ)
            except (ValueError, TypeError):
                continue
        raise ValueError(f"Cannot cast {value!r} to any of {args}")
    
    # Handle base types
    if target_type == bool:
        return value.lower() in ('true', '1', 'yes', 'on', True)
    elif target_type == int:
        return int(value)
    elif target_type == float:
        return float(value)
    elif target_type == str:
        return value
    else:
        raise TypeError(f"Unsupported type: {target_type}")

@dataclass
class EmailConfig(ConfigValue):
    smtp_server: Optional[str] = field(metadata={'env_var': 'SCHD_SMTP_SERVER'}, default=None)
    smtp_user: Optional[str] = field(metadata={'env_var': 'SCHD_SMTP_USER'}, default=None)
    smtp_password: Optional[str] = field(metadata={'env_var': 'SCHD_SMTP_PASS'}, default=None)
    from_addr: Optional[str] = field(metadata={'env_var': 'SCHD_SMTP_FROM'}, default=None)
    to_addr: Optional[str] = field(metadata={'env_var': 'SCHD_SMTP_TO'}, default=None)
    smtp_port: int = field(metadata={'env_var': 'SCHD_SMTP_PORT'}, default=25)
    smtp_starttls: bool = field(metadata={'env_var': 'SCHD_SMTP_TLS'}, default=False)


@dataclass
class JobConfig(ConfigValue):
    cls: str = field(metadata={"json": "class"})
    cron: str
    cmd: Optional[str] = None
    params: dict = field(default_factory=dict)
    timezone: Optional[str] = None
    queue: str = ''


@dataclass
class SchdConfig(ConfigValue):
    jobs: Dict[str, JobConfig] = field(default_factory=dict)
    scheduler_cls: str = 'LocalScheduler'
    scheduler_remote_host: Optional[str] = None
    worker_name: str = 'local'

    def __getitem__(self,key):
        # compatible to old fashion config['key']
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
        config = SchdConfig.from_dict(yaml.load(f, Loader=yaml.FullLoader))
        return config
