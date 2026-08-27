from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from typing import Optional

import psutil
from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from pydantic import BaseModel
from sse_starlette.sse import EventSourceResponse
from starlette.middleware.cors import CORSMiddleware

from ..config.schema import RouteConfig, get_config, save_config
from ..router.mc_router import RouteManager

logger = logging.getLogger(__name__)

app = FastAPI(title="Minegate API", version="1.0.0")

app.add_middleware(
    CORSMiddleware,          # type: ignore
    allow_origins=["*"],     # type: ignore
    allow_credentials=False, # type: ignore
    allow_methods=["*"],     # type: ignore
    allow_headers=["*"],     # type: ignore
)

_route_manager: Optional[RouteManager] = None
_start_time: float = time.time()
_last_net_check: Optional[dict] = None


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


class RouteCreate(BaseModel):
    subdomain: str
    host: str
    port: int = 25565


class RouteUpdate(BaseModel):
    host: Optional[str] = None
    port: Optional[int] = None


class RouteResponse(BaseModel):
    subdomain: str
    host: str
    port: int
    online: bool
    active_connections: int


@app.get("/route/{subdomain}", response_model=RouteResponse)
async def get_route(subdomain: str) -> RouteResponse:
    manager = get_route_manager()
    route = manager.get_route(subdomain)
    if route is None:
        raise HTTPException(status_code=404, detail=f"Route '{subdomain}' not found")

    online = await manager.is_server_online(route.host, route.port)
    return RouteResponse(
        subdomain=route.subdomain,
        host=route.host,
        port=route.port,
        online=online,
        active_connections=manager.get_active_connections(subdomain),
    )


@app.post("/route", response_model=RouteResponse)
async def create_route(route: RouteCreate) -> RouteResponse:
    manager = get_route_manager()
    config = get_config()

    existing = manager.get_route(route.subdomain)
    if existing:
        raise HTTPException(status_code=409, detail=f"Route '{route.subdomain}' already exists")

    new_route = RouteConfig(
        subdomain=route.subdomain,
        host=route.host,
        port=route.port,
    )
    await manager.add_route(new_route)
    config.routes.append(new_route)
    save_config(config)

    online = await manager.is_server_online(route.host, route.port)

    return RouteResponse(
        subdomain=route.subdomain,
        host=route.host,
        port=route.port,
        online=online,
        active_connections=0,
    )


@app.get("/routes")
async def list_routes():
    manager = get_route_manager()
    routes = manager.list_routes()

    async def event_generator():
        for route in routes:
            online = await manager.is_server_online(route.host, route.port)
            data = {
                "subdomain": route.subdomain,
                "host": route.host,
                "port": route.port,
                "online": online,
                "active_connections": manager.get_active_connections(route.subdomain),
            }
            yield {"event": "route", "data": str(data)}
        yield {"event": "complete", "data": "[]"}

    return EventSourceResponse(event_generator())


@app.delete("/route/{subdomain}")
async def delete_route(subdomain: str) -> dict:
    manager = get_route_manager()
    config = get_config()

    removed = await manager.remove_route(subdomain)
    if removed is None:
        raise HTTPException(status_code=404, detail=f"Route '{subdomain}' not found")

    config.routes = [r for r in config.routes if r.subdomain.lower() != subdomain.lower()]
    save_config(config)

    return {"message": f"Route '{subdomain}' deleted"}


@app.put("/route/{subdomain}", response_model=RouteResponse)
async def update_route(subdomain: str, update: RouteUpdate) -> RouteResponse:
    manager = get_route_manager()
    config = get_config()

    existing = manager.get_route(subdomain)
    if existing is None:
        raise HTTPException(status_code=404, detail=f"Route '{subdomain}' not found")

    updated_route = RouteConfig(
        subdomain=subdomain,
        host=update.host if update.host is not None else existing.host,
        port=update.port if update.port is not None else existing.port,
    )
    await manager.update_route(updated_route)

    for i, r in enumerate(config.routes):
        if r.subdomain.lower() == subdomain.lower():
            config.routes[i] = updated_route
            break
    save_config(config)

    online = await manager.is_server_online(updated_route.host, updated_route.port)

    return RouteResponse(
        subdomain=updated_route.subdomain,
        host=updated_route.host,
        port=updated_route.port,
        online=online,
        active_connections=manager.get_active_connections(subdomain),
    )

async def get_status_data() -> dict:
    manager = get_route_manager()
    config = get_config()
    process = psutil.Process(os.getpid())

    routes = manager.list_routes()
    route_statuses = []
    for route in routes:
        online = await manager.is_server_online(route.host, route.port)
        route_statuses.append({
            "subdomain": route.subdomain,
            "host": route.host,
            "port": route.port,
            "online": online,
            "active_connections": manager.get_active_connections(route.subdomain),
        })

    mem = process.memory_info()
    sys_mem = psutil.virtual_memory()
    cpu_freq = psutil.cpu_freq()
    cpu_times = process.cpu_times()
    net = psutil.net_io_counters()
    now = time.time()

    global _last_net_check
    sent_rate_bps = 0.0
    recv_rate_bps = 0.0
    if _last_net_check is not None:
        dt = now - _last_net_check["time"]
        if dt > 0:
            sent_rate_bps = (net.bytes_sent - _last_net_check["bytes_sent"]) / dt
            recv_rate_bps = (net.bytes_recv - _last_net_check["bytes_recv"]) / dt
    _last_net_check = {"time": now, "bytes_sent": net.bytes_sent, "bytes_recv": net.bytes_recv}

    total_connections = sum(r["active_connections"] for r in route_statuses)
    online_routes = sum(1 for r in route_statuses if r["online"])

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
            "ram_available_mb": round(sys_mem.available / (1024 * 1024), 2),
            "ram_percent": sys_mem.percent,
        },
        "throughput": {
            "total_bytes_sent": net.bytes_sent,
            "total_bytes_recv": net.bytes_recv,
            "bytes_sent_rate": round(sent_rate_bps, 1),
            "bytes_recv_rate": round(recv_rate_bps, 1),
            "bytes_sent_rate_human": _format_bytes_rate(sent_rate_bps),
            "bytes_recv_rate_human": _format_bytes_rate(recv_rate_bps),
        },
        "network": {
            "total_routes": len(route_statuses),
            "online_routes": online_routes,
            "offline_routes": len(route_statuses) - online_routes,
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
            await websocket.send_text(json.dumps(await get_status_data(), default=str))
            await asyncio.sleep(2)
    except WebSocketDisconnect:
        pass
    except Exception as e:
        logger.error(f"WebSocket error: {e}")


def _human_bytes(n: float) -> str:
    for unit in ("B/s", "KB/s", "MB/s", "GB/s"):
        if abs(n) < 1024:
            return f"{n:.1f} {unit}"
        n /= 1024
    return f"{n:.1f} TB/s"
