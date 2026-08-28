from __future__ import annotations

import asyncio
import logging
import random
import socket
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Optional

from mcproto.connection import TCPAsyncConnection
from mcproto.packets import async_read_packet, async_write_packet
from mcproto.packets.handshaking.handshake import Handshake as MCHandshake, NextState
from mcproto.packets.status.status import StatusResponse
from mcproto.packets.login.login import LoginDisconnect
from mcproto.types.chat import ChatMessage

from ..config.schema import (
    BackendConfig,
    Config,
    LoadBalanceStrategy,
    RouteConfig,
    get_config,
)
from ..protocol.utils import HANDSHAKE_SERVERBOUND_MAP

logger = logging.getLogger(__name__)

_executor = ThreadPoolExecutor(max_workers=8)
_STATUS_TTL = 5.0
_PING_TTL = 10.0
_PIPE_BUFFER = 65536


def _backend_key(subdomain: str, backend: BackendConfig) -> str:
    return f"{subdomain.lower()}|{backend.host}:{backend.port}"


class RouteManager:
    def __init__(self, config: Config):
        self.config = config
        self.routes: dict[str, RouteConfig] = {}
        self._backend_connections: dict[str, int] = {}
        self._status_cache: dict[str, tuple[bool, float]] = {}
        self._ping_cache: dict[str, tuple[float, float]] = {}
        self._round_robin_counters: dict[str, int] = {}
        self._lock = asyncio.Lock()
        self._tunnel_bytes_sent: dict[str, int] = {}
        self._tunnel_bytes_recv: dict[str, int] = {}
        self._tunnel_bytes_sent_prev: dict[str, int] = {}
        self._tunnel_bytes_recv_prev: dict[str, int] = {}
        self._tunnel_rate_time: Optional[float] = None
        for route in config.routes:
            self.routes[route.subdomain.lower()] = route

    async def select_backend(
        self, route: RouteConfig
    ) -> Optional[BackendConfig]:
        backends = route.backends
        if not backends:
            return None
        if len(backends) == 1:
            return backends[0]

        strategy = route.load_balance.strategy
        if strategy == LoadBalanceStrategy.random:
            return random.choice(backends)

        if strategy == LoadBalanceStrategy.round_robin:
            key = route.subdomain.lower()
            idx = self._round_robin_counters.get(key, 0)
            self._round_robin_counters[key] = (idx + 1) % len(backends)
            return backends[idx]

        if strategy == LoadBalanceStrategy.least_connections:
            return min(
                backends,
                key=lambda b: self._backend_connections.get(
                    _backend_key(route.subdomain, b), 0
                ),
            )

        if strategy == LoadBalanceStrategy.least_ping:
            now = time.monotonic()
            scored: list[tuple[float, BackendConfig]] = []
            for b in backends:
                bk = _backend_key(route.subdomain, b)
                cached = self._ping_cache.get(bk)
                if cached and (now - cached[1]) < _PING_TTL:
                    scored.append((cached[0], b))
                else:
                    scored.append((999.0, b))
            scored.sort(key=lambda x: x[0])
            return scored[0][1]

        if strategy == LoadBalanceStrategy.weighted_random:
            total_weight = sum(b.weight for b in backends)
            if total_weight <= 0:
                return random.choice(backends)
            pick = random.uniform(0, total_weight)
            cumulative = 0.0
            for b in backends:
                cumulative += b.weight
                if pick <= cumulative:
                    return b
            return backends[-1]

        return backends[0]

    async def increment_backend_connections(
        self, subdomain: str, backend: BackendConfig
    ) -> None:
        async with self._lock:
            key = _backend_key(subdomain, backend)
            self._backend_connections[key] = (
                self._backend_connections.get(key, 0) + 1
            )

    async def decrement_backend_connections(
        self, subdomain: str, backend: BackendConfig
    ) -> None:
        async with self._lock:
            key = _backend_key(subdomain, backend)
            if self._backend_connections.get(key, 0) > 0:
                self._backend_connections[key] -= 1

    def get_backend_connections(
        self, subdomain: str, backend: BackendConfig
    ) -> int:
        return self._backend_connections.get(_backend_key(subdomain, backend), 0)

    def get_active_connections(self, subdomain: str) -> int:
        total = 0
        prefix = f"{subdomain.lower()}|"
        for k, v in self._backend_connections.items():
            if k.startswith(prefix):
                total += v
        return total

    def record_tunnel_bytes(
        self, subdomain: str, bytes_sent: int, bytes_recv: int
    ) -> None:
        key = subdomain.lower()
        self._tunnel_bytes_sent[key] = (
            self._tunnel_bytes_sent.get(key, 0) + bytes_sent
        )
        self._tunnel_bytes_recv[key] = (
            self._tunnel_bytes_recv.get(key, 0) + bytes_recv
        )

    def get_tunnel_throughput(self) -> dict:
        now = time.monotonic()
        total_sent_rate = 0.0
        total_recv_rate = 0.0
        per_route = {}

        if self._tunnel_rate_time is not None:
            dt = now - self._tunnel_rate_time
            if dt > 0:
                all_keys = set(self._tunnel_bytes_sent) | set(
                    self._tunnel_bytes_recv
                )
                for key in all_keys:
                    cur_sent = self._tunnel_bytes_sent.get(key, 0)
                    cur_recv = self._tunnel_bytes_recv.get(key, 0)
                    prev_sent = self._tunnel_bytes_sent_prev.get(key, 0)
                    prev_recv = self._tunnel_bytes_recv_prev.get(key, 0)
                    sent_rate = (cur_sent - prev_sent) / dt
                    recv_rate = (cur_recv - prev_recv) / dt
                    total_sent_rate += sent_rate
                    total_recv_rate += recv_rate
                    per_route[key] = {
                        "bytes_sent": cur_sent,
                        "bytes_recv": cur_recv,
                        "bytes_sent_rate": round(sent_rate, 1),
                        "bytes_recv_rate": round(recv_rate, 1),
                    }

        self._tunnel_bytes_sent_prev = dict(self._tunnel_bytes_sent)
        self._tunnel_bytes_recv_prev = dict(self._tunnel_bytes_recv)
        self._tunnel_rate_time = now

        return {
            "total_bytes_sent": sum(self._tunnel_bytes_sent.values()),
            "total_bytes_recv": sum(self._tunnel_bytes_recv.values()),
            "bytes_sent_rate": round(total_sent_rate, 1),
            "bytes_recv_rate": round(total_recv_rate, 1),
            "per_route": per_route,
        }

    def get_route(self, subdomain: str) -> Optional[RouteConfig]:
        return self.routes.get(subdomain.lower())

    async def add_route(self, route: RouteConfig) -> None:
        async with self._lock:
            self.routes[route.subdomain.lower()] = route

    async def remove_route(self, subdomain: str) -> Optional[RouteConfig]:
        async with self._lock:
            return self.routes.pop(subdomain.lower(), None)

    async def update_route(self, route: RouteConfig) -> None:
        async with self._lock:
            self.routes[route.subdomain.lower()] = route

    def list_routes(self) -> list[RouteConfig]:
        return list(self.routes.values())

    def extract_subdomain(self, server_address: str) -> Optional[str]:
        address = server_address.split("\x00")[0]
        address = address.split("///")[0]
        address = address.lower().rstrip(".")

        root = self.config.root_domain.lower().rstrip(".")
        if address == root:
            return None
        if address.endswith("." + root):
            subdomain = address[: -(len(root) + 1)]
            return subdomain
        return None

    async def is_server_online(self, host: str, port: int) -> bool:
        key = f"{host}:{port}"
        now = time.monotonic()
        cached = self._status_cache.get(key)
        if cached and (now - cached[1]) < _STATUS_TTL:
            return cached[0]

        loop = asyncio.get_running_loop()

        def _check():
            try:
                with socket.create_connection((host, port), timeout=2):
                    return True
            except Exception:
                return False

        result = await loop.run_in_executor(_executor, _check)
        self._status_cache[key] = (result, now)
        return result

    async def measure_ping(self, host: str, port: int) -> float:
        key = f"{host}:{port}"
        now = time.monotonic()
        cached = self._ping_cache.get(key)
        if cached and (now - cached[1]) < _PING_TTL:
            return cached[0]

        loop = asyncio.get_running_loop()

        def _measure():
            try:
                start = time.monotonic()
                with socket.create_connection((host, port), timeout=3):
                    return (time.monotonic() - start) * 1000.0
            except Exception:
                return -1.0

        result = await loop.run_in_executor(_executor, _measure)
        self._ping_cache[key] = (result, now)
        return result


def make_status_response(
    protocol_version: int,
    description: str,
    players_max: int = 0,
    players_online: int = 0,
    icon: Optional[str] = None,
) -> dict:
    status = {
        "version": {"name": "1.20.4", "protocol": protocol_version},
        "players": {"max": players_max, "online": players_online},
        "description": {"text": description},
    }
    if icon:
        status["favicon"] = icon
    return status


async def pipe_streams(
    client_reader: asyncio.StreamReader,
    client_writer: asyncio.StreamWriter,
    server_reader: asyncio.StreamReader,
    server_writer: asyncio.StreamWriter,
    subdomain: Optional[str] = None,
    route_manager: Optional[RouteManager] = None,
) -> None:
    async def pipe(reader, writer, is_uplink: bool):
        total = 0
        try:
            while True:
                data = await reader.read(_PIPE_BUFFER)
                if not data:
                    break
                total += len(data)
                writer.write(data)
                await writer.drain()
        except Exception:
            pass
        finally:
            if not writer.is_closing():
                writer.close()
        if subdomain and route_manager and total > 0:
            if is_uplink:
                route_manager.record_tunnel_bytes(
                    subdomain, bytes_sent=total, bytes_recv=0
                )
            else:
                route_manager.record_tunnel_bytes(
                    subdomain, bytes_sent=0, bytes_recv=total
                )

    await asyncio.gather(
        pipe(client_reader, server_writer, is_uplink=True),
        pipe(server_reader, client_writer, is_uplink=False),
        return_exceptions=True,
    )


async def _try_connect(
    host: str, port: int, timeout: float = 5.0
) -> Optional[tuple[asyncio.StreamReader, asyncio.StreamWriter]]:
    try:
        return await asyncio.wait_for(
            asyncio.open_connection(host, port), timeout=timeout
        )
    except Exception:
        return None


async def handle_client(
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
    route_manager: RouteManager,
) -> None:
    addr = writer.get_extra_info("peername")
    logger.info(f"New connection from {addr}")

    conn = TCPAsyncConnection(reader=reader, writer=writer, timeout=10)
    config = get_config()

    try:
        handshake = await async_read_packet(conn, HANDSHAKE_SERVERBOUND_MAP)
        if not isinstance(handshake, MCHandshake):
            logger.warning(f"Unexpected packet from {addr}: {type(handshake)}")
            return

        subdomain = route_manager.extract_subdomain(handshake.server_address)

        if subdomain is None:
            if handshake.next_state == NextState.STATUS:
                status_data = make_status_response(
                    handshake.protocol_version,
                    config.root_domain_motd,
                    icon=config.root_domain_icon,
                )
                await async_write_packet(conn, StatusResponse(data=status_data))
            else:
                reason = ChatMessage(
                    {
                        "text": f"\u00A7cCould not connect you to Minegate!\u00A7r\n"
                        f"You must join via a subdomain\u00A7r\n"
                        f"Example: play.{config.root_domain}"
                    }
                )
                await async_write_packet(conn, LoginDisconnect(reason=reason))
            return

        route = route_manager.get_route(subdomain)
        if route is None:
            if handshake.next_state == NextState.STATUS:
                status_data = make_status_response(
                    handshake.protocol_version,
                    config.not_found_motd,
                    icon=config.not_found_icon,
                )
                await async_write_packet(conn, StatusResponse(data=status_data))
            else:
                reason = ChatMessage({"text": config.not_found_motd})
                await async_write_packet(conn, LoginDisconnect(reason=reason))
            return

        if not route.backends:
            if handshake.next_state == NextState.STATUS:
                status_data = make_status_response(
                    handshake.protocol_version,
                    config.server_offline_motd % subdomain,
                    icon=config.server_offline_icon,
                )
                await async_write_packet(conn, StatusResponse(data=status_data))
            else:
                reason = ChatMessage(
                    {"text": config.server_offline_motd % subdomain}
                )
                await async_write_packet(conn, LoginDisconnect(reason=reason))
            return

        if handshake.next_state == NextState.STATUS:
            backend = await route_manager.select_backend(route)
            if backend is None:
                status_data = make_status_response(
                    handshake.protocol_version,
                    config.server_offline_motd % subdomain,
                    icon=config.server_offline_icon,
                )
                await async_write_packet(conn, StatusResponse(data=status_data))
                return

            online = await route_manager.is_server_online(
                backend.host, backend.port
            )
            if not online:
                status_data = make_status_response(
                    handshake.protocol_version,
                    config.server_offline_motd % subdomain,
                    icon=config.server_offline_icon,
                )
                await async_write_packet(conn, StatusResponse(data=status_data))
                return

            result = await _try_connect(backend.host, backend.port)
            if result is None:
                status_data = make_status_response(
                    handshake.protocol_version,
                    config.server_offline_motd % subdomain,
                    icon=config.server_offline_icon,
                )
                await async_write_packet(conn, StatusResponse(data=status_data))
                return

            server_reader, server_writer = result
            server_conn = TCPAsyncConnection(
                reader=server_reader, writer=server_writer, timeout=10
            )
            await async_write_packet(server_conn, handshake)
            await pipe_streams(
                reader, writer, server_reader, server_writer, subdomain, route_manager
            )
            return

        if handshake.next_state == NextState.LOGIN:
            online_backends: list[BackendConfig] = []
            for b in route.backends:
                if await route_manager.is_server_online(b.host, b.port):
                    online_backends.append(b)

            if not online_backends:
                reason = ChatMessage(
                    {"text": config.server_offline_motd % subdomain}
                )
                await async_write_packet(conn, LoginDisconnect(reason=reason))
                return

            candidate_route = RouteConfig(
                subdomain=route.subdomain,
                backends=online_backends,
                load_balance=route.load_balance,
            )
            backend = await route_manager.select_backend(candidate_route)
            if backend is None:
                reason = ChatMessage(
                    {"text": config.server_offline_motd % subdomain}
                )
                await async_write_packet(conn, LoginDisconnect(reason=reason))
                return

            result = await _try_connect(backend.host, backend.port)
            if result is None:
                reason = ChatMessage(
                    {"text": "Failed to connect to backend server"}
                )
                await async_write_packet(conn, LoginDisconnect(reason=reason))
                return

            server_reader, server_writer = result
            server_conn = TCPAsyncConnection(
                reader=server_reader, writer=server_writer, timeout=30
            )
            await async_write_packet(server_conn, handshake)

            await route_manager.increment_backend_connections(subdomain, backend)
            try:
                await pipe_streams(
                    reader,
                    writer,
                    server_reader,
                    server_writer,
                    subdomain,
                    route_manager,
                )
            finally:
                await route_manager.decrement_backend_connections(
                    subdomain, backend
                )

    except asyncio.TimeoutError:
        logger.warning(f"Connection timeout from {addr}")
    except Exception as e:
        logger.error(f"Error handling client {addr}: {e}")
    finally:
        if not writer.is_closing():
            writer.close()


class MCRouter:
    def __init__(self):
        self.config = get_config()
        self.route_manager = RouteManager(self.config)
        self.server: Optional[asyncio.AbstractServer] = None

    async def start(self) -> None:
        self.server = await asyncio.start_server(
            lambda r, w: handle_client(r, w, self.route_manager),
            self.config.mc_listen_host,
            self.config.mc_listen_port,
        )
        addr = self.server.sockets[0].getsockname()
        logger.info(f"MC Router listening on {addr[0]}:{addr[1]}")

    async def stop(self) -> None:
        if self.server:
            self.server.close()
            await self.server.wait_closed()
            logger.info("MC Router stopped")
