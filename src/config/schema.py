from __future__ import annotations

import os
from pathlib import Path
from typing import Optional

import yaml
from pydantic import BaseModel, Field


class RouteConfig(BaseModel):
    subdomain: str
    host: str
    port: int = 25565


class Config(BaseModel):
    root_domain: str = "mc.example.net"
    mc_listen_host: str = "0.0.0.0"
    mc_listen_port: int = 25565
    api_listen_host: str = "0.0.0.0"
    api_listen_port: int = 8000
    root_domain_motd: str = "\u00A7aWelcome to Minegate\u00A7r\n\u00A7fA fast \u00A7ePy\u00A79thon \u00A7fserver router"
    root_domain_icon: Optional[str] = None
    not_found_motd: str = "Server does not exist"
    not_found_icon: Optional[str] = None
    server_offline_motd: str = "Server %s is offline!"
    server_offline_icon: Optional[str] = None
    routes: list[RouteConfig] = Field(default_factory=list)


_config: Optional[Config] = None


def get_config_path() -> Path:
    env_path = os.environ.get("MINEGATE_CONFIG")
    if env_path:
        return Path(env_path)
    return Path("config.yaml")


def load_config() -> Config:
    global _config
    path = get_config_path()
    if path.exists():
        with open(path, "r") as f:
            data = yaml.safe_load(f) or {}
        _config = Config(**data)
    else:
        _config = Config()
        save_config(_config)
    return _config


def save_config(config: Config) -> None:
    global _config
    _config = config
    path = get_config_path()
    data = config.model_dump()
    with open(path, "w") as f:
        yaml.dump(data, f, default_flow_style=False, sort_keys=False)


def get_config() -> Config:
    global _config
    if _config is None:
        return load_config()
    return _config
