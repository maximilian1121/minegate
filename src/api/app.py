from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from typing import Optional

import psutil
from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from pydantic import BaseModel, Field
from sse_starlette.sse import EventSourceResponse
from starlette.middleware.cors import CORSMiddleware
from starlette.responses import HTMLResponse

from ..config.schema import (
    BackendConfig,
    LoadBalanceConfig,
    LoadBalanceStrategy,
    RouteConfig,
    get_config,
    save_config,
)
from ..router.mc_router import RouteManager, _backend_key

logger = logging.getLogger(__name__)

app = FastAPI(title="Minegate API", version="2.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

_route_manager: Optional[RouteManager] = None
_start_time: float = time.time()


def set_route_manager(manager: RouteManager) -> None:
    global _route_manager
    _route_manager = manager


def _format_bytes_rate(bps: float) -> str:
    for unit in ("B/s", "KB/s", "MB/s", "GB/s"):
        if abs(bps) < 1024:
            return f"{bps:.1f} {unit}"
        bps /= 1024
    return f"{bps:.1f} TB/s"


def get_route_manager() -> RouteManager:
    if _route_manager is None:
        raise RuntimeError("RouteManager not initialized")
    return _route_manager


class BackendCreate(BaseModel):
    host: str
    port: int = 25565
    weight: int = 1
    priority: int = 0


class BackendResponse(BaseModel):
    host: str
    port: int
    weight: int
    priority: int
    online: bool
    active_connections: int
    ping_ms: Optional[float] = None


class LoadBalanceCreate(BaseModel):
    strategy: LoadBalanceStrategy = LoadBalanceStrategy.round_robin


class RouteCreate(BaseModel):
    subdomain: str
    backends: list[BackendCreate] = Field(default_factory=list)
    load_balance: Optional[LoadBalanceCreate] = None


class RouteUpdate(BaseModel):
    backends: Optional[list[BackendCreate]] = None
    load_balance: Optional[LoadBalanceCreate] = None


class RouteResponse(BaseModel):
    subdomain: str
    backends: list[BackendResponse]
    active_connections: int
    load_balance_strategy: str


def _build_backend_response(
    manager: RouteManager, subdomain: str, b: BackendConfig
) -> BackendResponse:
    return BackendResponse(
        host=b.host,
        port=b.port,
        weight=b.weight,
        priority=b.priority,
        online=False,
        active_connections=manager.get_backend_connections(subdomain, b),
    )


async def _enrich_backend_responses(
    manager: RouteManager, subdomain: str, backends: list[BackendConfig]
) -> list[BackendResponse]:
    responses: list[BackendResponse] = []
    for b in backends:
        resp = _build_backend_response(manager, subdomain, b)
        resp.online = await manager.is_server_online(b.host, b.port)
        resp.ping_ms = await manager.measure_ping(b.host, b.port)
        if resp.ping_ms < 0:
            resp.ping_ms = None
        responses.append(resp)
    return responses


def _route_response(
    manager: RouteManager, route: RouteConfig, backend_responses: list[BackendResponse]
) -> RouteResponse:
    total_conns = sum(br.active_connections for br in backend_responses)
    return RouteResponse(
        subdomain=route.subdomain,
        backends=backend_responses,
        active_connections=total_conns,
        load_balance_strategy=route.load_balance.strategy.value,
    )


@app.get("/route/{subdomain}", response_model=RouteResponse)
async def get_route(subdomain: str) -> RouteResponse:
    manager = get_route_manager()
    route = manager.get_route(subdomain)
    if route is None:
        raise HTTPException(
            status_code=404, detail=f"Route '{subdomain}' not found"
        )
    backend_responses = await _enrich_backend_responses(
        manager, subdomain, route.backends
    )
    return _route_response(manager, route, backend_responses)


@app.post("/route", response_model=RouteResponse, status_code=201)
async def create_route(route: RouteCreate) -> RouteResponse:
    manager = get_route_manager()
    config = get_config()

    existing = manager.get_route(route.subdomain)
    if existing:
        raise HTTPException(
            status_code=409, detail=f"Route '{route.subdomain}' already exists"
        )

    if not route.backends:
        raise HTTPException(
            status_code=422, detail="At least one backend is required"
        )

    lb = route.load_balance or LoadBalanceCreate()
    new_route = RouteConfig(
        subdomain=route.subdomain,
        backends=[
            BackendConfig(
                host=b.host, port=b.port, weight=b.weight, priority=b.priority
            )
            for b in route.backends
        ],
        load_balance=LoadBalanceConfig(strategy=lb.strategy),
    )
    await manager.add_route(new_route)
    config.routes.append(new_route)
    save_config(config)

    backend_responses = await _enrich_backend_responses(
        manager, route.subdomain, new_route.backends
    )
    return _route_response(manager, new_route, backend_responses)


@app.get("/routes")
async def list_routes():
    manager = get_route_manager()
    routes = manager.list_routes()

    async def event_generator():
        for route in routes:
            backend_responses = await _enrich_backend_responses(
                manager, route.subdomain, route.backends
            )
            resp = _route_response(manager, route, backend_responses)
            yield {"event": "route", "data": resp.model_dump_json()}
        yield {"event": "complete", "data": "[]"}

    return EventSourceResponse(event_generator())


@app.delete("/route/{subdomain}")
async def delete_route(subdomain: str) -> dict:
    manager = get_route_manager()
    config = get_config()

    removed = await manager.remove_route(subdomain)
    if removed is None:
        raise HTTPException(
            status_code=404, detail=f"Route '{subdomain}' not found"
        )

    config.routes = [
        r for r in config.routes if r.subdomain.lower() != subdomain.lower()
    ]
    save_config(config)

    return {"message": f"Route '{subdomain}' deleted"}


@app.put("/route/{subdomain}", response_model=RouteResponse)
async def update_route(subdomain: str, update: RouteUpdate) -> RouteResponse:
    manager = get_route_manager()
    config = get_config()

    existing = manager.get_route(subdomain)
    if existing is None:
        raise HTTPException(
            status_code=404, detail=f"Route '{subdomain}' not found"
        )

    new_backends = existing.backends
    if update.backends is not None:
        if not update.backends:
            raise HTTPException(
                status_code=422, detail="At least one backend is required"
            )
        new_backends = [
            BackendConfig(
                host=b.host, port=b.port, weight=b.weight, priority=b.priority
            )
            for b in update.backends
        ]

    new_lb = existing.load_balance
    if update.load_balance is not None:
        new_lb = LoadBalanceConfig(strategy=update.load_balance.strategy)

    updated_route = RouteConfig(
        subdomain=existing.subdomain,
        backends=new_backends,
        load_balance=new_lb,
    )
    await manager.update_route(updated_route)

    for i, r in enumerate(config.routes):
        if r.subdomain.lower() == subdomain.lower():
            config.routes[i] = updated_route
            break
    save_config(config)

    backend_responses = await _enrich_backend_responses(
        manager, subdomain, updated_route.backends
    )
    return _route_response(manager, updated_route, backend_responses)


@app.post("/route/{subdomain}/backend", response_model=BackendResponse, status_code=201)
async def add_backend(subdomain: str, backend: BackendCreate) -> BackendResponse:
    manager = get_route_manager()
    config = get_config()

    route = manager.get_route(subdomain)
    if route is None:
        raise HTTPException(
            status_code=404, detail=f"Route '{subdomain}' not found"
        )

    for b in route.backends:
        if b.host == backend.host and b.port == backend.port:
            raise HTTPException(
                status_code=409,
                detail=f"Backend {backend.host}:{backend.port} already exists",
            )

    new_backend = BackendConfig(
        host=backend.host,
        port=backend.port,
        weight=backend.weight,
        priority=backend.priority,
    )
    updated_route = RouteConfig(
        subdomain=route.subdomain,
        backends=[*route.backends, new_backend],
        load_balance=route.load_balance,
    )
    await manager.update_route(updated_route)

    for i, r in enumerate(config.routes):
        if r.subdomain.lower() == subdomain.lower():
            config.routes[i] = updated_route
            break
    save_config(config)

    online = await manager.is_server_online(backend.host, backend.port)
    ping_ms = await manager.measure_ping(backend.host, backend.port)

    return BackendResponse(
        host=backend.host,
        port=backend.port,
        weight=backend.weight,
        priority=backend.priority,
        online=online,
        active_connections=0,
        ping_ms=ping_ms if ping_ms >= 0 else None,
    )


@app.delete("/route/{subdomain}/backend/{host}:{port}")
async def remove_backend(subdomain: str, host: str, port: int) -> dict:
    manager = get_route_manager()
    config = get_config()

    route = manager.get_route(subdomain)
    if route is None:
        raise HTTPException(
            status_code=404, detail=f"Route '{subdomain}' not found"
        )

    new_backends = [
        b for b in route.backends if not (b.host == host and b.port == port)
    ]
    if len(new_backends) == len(route.backends):
        raise HTTPException(
            status_code=404,
            detail=f"Backend {host}:{port} not found in route '{subdomain}'",
        )
    if not new_backends:
        raise HTTPException(
            status_code=422,
            detail="Cannot remove the last backend. Delete the route instead.",
        )

    updated_route = RouteConfig(
        subdomain=route.subdomain,
        backends=new_backends,
        load_balance=route.load_balance,
    )
    await manager.update_route(updated_route)

    for i, r in enumerate(config.routes):
        if r.subdomain.lower() == subdomain.lower():
            config.routes[i] = updated_route
            break
    save_config(config)

    return {"message": f"Backend {host}:{port} removed from route '{subdomain}'"}


async def get_status_data() -> dict:
    manager = get_route_manager()
    config = get_config()
    process = psutil.Process(os.getpid())

    routes = manager.list_routes()
    route_statuses = []
    total_online_backends = 0
    total_backends = 0
    for route in routes:
        backend_statuses = []
        for b in route.backends:
            total_backends += 1
            online = await manager.is_server_online(b.host, b.port)
            if online:
                total_online_backends += 1
            backend_statuses.append({
                "host": b.host,
                "port": b.port,
                "weight": b.weight,
                "priority": b.priority,
                "online": online,
                "active_connections": manager.get_backend_connections(
                    route.subdomain, b
                ),
            })
        route_statuses.append({
            "subdomain": route.subdomain,
            "load_balance_strategy": route.load_balance.strategy.value,
            "backends": backend_statuses,
            "active_connections": manager.get_active_connections(route.subdomain),
        })

    mem = process.memory_info()
    sys_mem = psutil.virtual_memory()
    cpu_freq = psutil.cpu_freq()
    cpu_times = process.cpu_times()

    tunnel_throughput = manager.get_tunnel_throughput()
    sent_rate_bps = tunnel_throughput["bytes_sent_rate"]
    recv_rate_bps = tunnel_throughput["bytes_recv_rate"]

    total_connections = sum(r["active_connections"] for r in route_statuses)

    uptime_s = time.time() - _start_time
    days = int(uptime_s // 86400)
    hours = int((uptime_s % 86400) // 3600)
    mins = int((uptime_s % 3600) // 60)
    secs = int(uptime_s % 60)

    return {
        "status": "ok",
        "root_domain": config.root_domain,
        "uptime": f"{days}d {hours}h {mins}m {secs}s",
        "uptime_seconds": round(uptime_s),
        "process": {
            "pid": os.getpid(),
            "ram_mb": round(mem.rss / (1024 * 1024), 2),
            "ram_vms_mb": round(mem.vms / (1024 * 1024), 2),
            "cpu_percent": round(process.cpu_percent(interval=0), 1),
            "cpu_user": round(cpu_times.user, 2),
            "cpu_system": round(cpu_times.system, 2),
            "threads": process.num_threads(),
            "open_fds": process.num_fds(),
        },
        "system": {
            "cpu_count": psutil.cpu_count(),
            "cpu_count_physical": psutil.cpu_count(logical=False),
            "cpu_percent": psutil.cpu_percent(interval=0),
            "cpu_freq_mhz": round(cpu_freq.current, 0) if cpu_freq else None,
            "ram_total_mb": round(sys_mem.total / (1024 * 1024), 2),
            "ram_used_mb": round(sys_mem.used / (1024 * 1024), 2),
            "ram_available_mb": round(
                sys_mem.available / (1024 * 1024), 2
            ),
            "ram_percent": sys_mem.percent,
        },
        "throughput": {
            "total_bytes_sent": tunnel_throughput["total_bytes_sent"],
            "total_bytes_recv": tunnel_throughput["total_bytes_recv"],
            "bytes_sent_rate": sent_rate_bps,
            "bytes_recv_rate": recv_rate_bps,
            "bytes_sent_rate_human": _format_bytes_rate(sent_rate_bps),
            "bytes_recv_rate_human": _format_bytes_rate(recv_rate_bps),
            "per_route": tunnel_throughput["per_route"],
        },
        "network": {
            "total_routes": len(route_statuses),
            "total_backends": total_backends,
            "online_backends": total_online_backends,
            "offline_backends": total_backends - total_online_backends,
            "total_connections": total_connections,
            "root_domain": config.root_domain,
        },
        "routes": route_statuses,
    }


@app.get("/health")
async def health() -> dict:
    return await get_status_data()


@app.websocket("/ws/status")
async def ws_status(websocket: WebSocket):
    await websocket.accept()

    try:
        while True:
            await websocket.send_text(
                json.dumps(await get_status_data(), default=str)
            )
            await asyncio.sleep(2)
    except WebSocketDisconnect:
        pass
    except Exception as e:
        logger.error(f"WebSocket error: {e}")
