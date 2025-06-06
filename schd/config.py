import yaml
from pydantic import BaseModel


class SchdConfig(BaseModel):
    db_url: str = "sqlite:///./schd.sqlite3"


def read_config(filepath:str=None) -> SchdConfig:
    if filepath is None:
        filepath = 'conf/schd.yaml'

    with open(filepath, "r") as f:
        config_dict = yaml.safe_load(f)

    config = SchdConfig(**config_dict)
    return config
