from __future__ import annotations

import logging
import os
from enum import Enum
from pathlib import Path
from typing import Optional

import yaml
from pydantic import BaseModel, Field, model_validator

logger = logging.getLogger(__name__)


class LoadBalanceStrategy(str, Enum):
    round_robin = "round_robin"
    least_connections = "least_connections"
    least_ping = "least_ping"
    weighted_random = "weighted_random"
    random = "random"


class BackendConfig(BaseModel):
    host: str
    port: int = 25565
    weight: int = 1
    priority: int = 0


class LoadBalanceConfig(BaseModel):
    strategy: LoadBalanceStrategy = LoadBalanceStrategy.round_robin


class RouteConfig(BaseModel):
    subdomain: str
    backends: list[BackendConfig] = Field(default_factory=list)
    load_balance: LoadBalanceConfig = Field(default_factory=LoadBalanceConfig)

    @model_validator(mode="before")
    @classmethod
    def migrate_legacy_format(cls, values: dict) -> dict:
        if "backends" not in values or not values["backends"]:
            host = values.pop("host", None)
            port = values.pop("port", 25565)
            if host is not None:
                values["backends"] = [{"host": host, "port": port}]
        lb = values.get("load_balance")
        if isinstance(lb, str):
            values["load_balance"] = {"strategy": lb}
        return values


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


def _has_legacy_routes(data: dict) -> bool:
    routes = data.get("routes")
    if not isinstance(routes, list):
        return False
    for r in routes:
        if isinstance(r, dict) and "host" in r:
            return True
    return False


def load_config() -> Config:
    global _config
    path = get_config_path()
    if path.exists():
        with open(path, "r") as f:
            data = yaml.safe_load(f) or {}
        migrated = _has_legacy_routes(data)
        _config = Config(**data)
        if migrated:
            logger.info("Migrating legacy route config to multi-backend format")
            save_config(_config)
    else:
        _config = Config()
        save_config(_config)
    return _config


def save_config(config: Config) -> None:
    global _config
    _config = config
    path = get_config_path()
    data = config.model_dump(mode="json")
    with open(path, "w") as f:
        yaml.dump(data, f, default_flow_style=False, sort_keys=False)


def get_config() -> Config:
    global _config
    if _config is None:
        return load_config()
    return _config
