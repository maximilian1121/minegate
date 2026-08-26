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

from ..config.schema import Config, RouteConfig, get_config, save_config
from ..router.mc_router import RouteManager

logger = logging.getLogger(__name__)

app = FastAPI(title="Minegate API", version="1.0.0")

_route_manager: Optional[RouteManager] = None
_start_time: float = time.time()


def set_route_manager(manager: RouteManager) -> None:
    global _route_manager
    _route_manager = manager


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


async def check_server_status(host: str, port: int) -> bool:
    try:
        _, writer = await asyncio.wait_for(
            asyncio.open_connection(host, port), timeout=3.0
        )
        writer.close()
        await writer.wait_closed()
        return True
    except Exception:
        return False


@app.get("/route/{subdomain}", response_model=RouteResponse)
async def get_route(subdomain: str) -> RouteResponse:
    manager = get_route_manager()
    route = await manager.get_route(subdomain)
    if route is None:
        raise HTTPException(status_code=404, detail=f"Route '{subdomain}' not found")

    online = await check_server_status(route.host, route.port)
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

    existing = await manager.get_route(route.subdomain)
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

    online = await check_server_status(route.host, route.port)

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
    routes = await manager.list_routes()

    async def event_generator():
        for route in routes:
            online = await check_server_status(route.host, route.port)
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

    existing = await manager.get_route(subdomain)
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

    online = await check_server_status(updated_route.host, updated_route.port)

    return RouteResponse(
        subdomain=updated_route.subdomain,
        host=updated_route.host,
        port=updated_route.port,
        online=online,
        active_connections=manager.get_active_connections(subdomain),
    )


@app.get("/health")
async def health() -> dict:
    return {"status": "ok"}


@app.websocket("/ws/status")
async def ws_status(websocket: WebSocket):
    await websocket.accept()
    manager = get_route_manager()
    config = get_config()
    process = psutil.Process(os.getpid())
    net_prev = psutil.net_io_counters()
    time_prev = time.monotonic()
    ticks = 0

    try:
        while True:
            now_mono = time.monotonic()
            now_wall = time.time()
            dt = now_mono - time_prev
            time_prev = now_mono
            ticks += 1

            routes = await manager.list_routes()
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
            mem_full = process.memory_full_info()
            sys_mem = psutil.virtual_memory()
            cpu_pct = process.cpu_percent(interval=0)
            cpu_freq = psutil.cpu_freq()
            cpu_times = process.cpu_times()
            net_now = psutil.net_io_counters()

            total_connections = sum(r["active_connections"] for r in route_statuses)
            online_routes = sum(1 for r in route_statuses if r["online"])

            uptime_s = now_wall - _start_time
            days = int(uptime_s // 86400)
            hours = int((uptime_s % 86400) // 3600)
            mins = int((uptime_s % 3600) // 60)
            secs = int(uptime_s % 60)

            bytes_sent_rate = (net_now.bytes_sent - net_prev.bytes_sent) / dt if dt > 0 else 0
            bytes_recv_rate = (net_now.bytes_recv - net_prev.bytes_recv) / dt if dt > 0 else 0
            pkts_sent_rate = (net_now.packets_sent - net_prev.packets_sent) / dt if dt > 0 else 0
            pkts_recv_rate = (net_now.packets_recv - net_prev.packets_recv) / dt if dt > 0 else 0
            net_prev = net_now

            ctx_voluntary = getattr(cpu_times, "voluntary", 0)
            ctx_involuntary = getattr(cpu_times, "involuntary", 0)

            status = {
                "uptime": f"{days}d {hours}h {mins}m {secs}s",
                "uptime_seconds": round(uptime_s),
                "ticks": ticks,
                "process": {
                    "pid": os.getpid(),
                    "ram_mb": round(mem.rss / (1024 * 1024), 2),
                    "ram_vms_mb": round(mem.vms / (1024 * 1024), 2),
                    "ram_shared_mb": round(getattr(mem, "shared", 0) / (1024 * 1024), 2),
                    "ram_peak_mb": round(getattr(mem_full, "peak_wset", mem.rss) / (1024 * 1024), 2),
                    "cpu_percent": round(cpu_pct, 1),
                    "cpu_user": round(cpu_times.user, 2),
                    "cpu_system": round(cpu_times.system, 2),
                    "threads": process.num_threads(),
                    "ctx_switches_voluntary": ctx_voluntary,
                    "ctx_switches_involuntary": ctx_involuntary,
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
                    "ram_buffers_mb": round(getattr(sys_mem, "buffers", 0) / (1024 * 1024), 2),
                    "ram_cached_mb": round(getattr(sys_mem, "cached", 0) / (1024 * 1024), 2),
                },
                "throughput": {
                    "bytes_sent_per_sec": round(bytes_sent_rate),
                    "bytes_recv_per_sec": round(bytes_recv_rate),
                    "bytes_sent_rate_human": _human_bytes(bytes_sent_rate),
                    "bytes_recv_rate_human": _human_bytes(bytes_recv_rate),
                    "packets_sent_per_sec": round(pkts_sent_rate, 1),
                    "packets_recv_per_sec": round(pkts_recv_rate, 1),
                    "total_bytes_sent": net_now.bytes_sent,
                    "total_bytes_recv": net_now.bytes_recv,
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

            await websocket.send_text(json.dumps(status, default=str))
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
