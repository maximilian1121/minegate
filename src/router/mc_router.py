from __future__ import annotations

import asyncio
import logging
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

from ..config.schema import Config, RouteConfig, get_config
from ..protocol.utils import HANDSHAKE_SERVERBOUND_MAP

logger = logging.getLogger(__name__)

_executor = ThreadPoolExecutor(max_workers=8)
_STATUS_TTL = 5.0
_PIPE_BUFFER = 65536


class RouteManager:
    def __init__(self, config: Config):
        self.config = config
        self.routes: dict[str, RouteConfig] = {}
        self.active_connections: dict[str, int] = {}
        self._status_cache: dict[str, tuple[bool, float]] = {}
        self._lock = asyncio.Lock()
        for route in config.routes:
            self.routes[route.subdomain.lower()] = route

    async def increment_connections(self, subdomain: str) -> None:
        async with self._lock:
            key = subdomain.lower()
            self.active_connections[key] = self.active_connections.get(key, 0) + 1

    async def decrement_connections(self, subdomain: str) -> None:
        async with self._lock:
            key = subdomain.lower()
            if self.active_connections.get(key, 0) > 0:
                self.active_connections[key] -= 1

    def get_active_connections(self, subdomain: str) -> int:
        return self.active_connections.get(subdomain.lower(), 0)

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
        # strip legacy BungeeCord/Velocity IP forwarding junk if present
        # format: host///real_ip///uuid///forwarding_data
        address = server_address.split("\x00")[0]  # also nuke FML null byte garbage while we're here
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
                with socket.create_connection((host, port), timeout=2) as s:
                    return True
            except Exception:
                return False

        result = await loop.run_in_executor(_executor, _check)
        self._status_cache[key] = (result, now)
        return result


def make_status_response(protocol_version: int, description: str, players_max: int = 0, players_online: int = 0, icon: Optional[str] = None) -> dict:
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
) -> None:
    async def pipe(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        try:
            while True:
                data = await reader.read(_PIPE_BUFFER)
                if not data:
                    break
                writer.write(data)
                await writer.drain()
        except Exception:
            pass
        finally:
            if not writer.is_closing():
                writer.close()

    await asyncio.gather(
        pipe(client_reader, server_writer),
        pipe(server_reader, client_writer),
        return_exceptions=True,
    )


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
                reason = ChatMessage({"text": f"\u00A7cCould not connect you to Minegate!\u00A7r\nYou must join via a subdomain\u00A7r\nExample: play.{config.root_domain}"})
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

        if handshake.next_state == NextState.STATUS:
            online = await route_manager.is_server_online(route.host, route.port)
            if not online:
                status_data = make_status_response(
                    handshake.protocol_version,
                    config.server_offline_motd % subdomain,
                    icon=config.server_offline_icon,
                )
                await async_write_packet(conn, StatusResponse(data=status_data))
                return

            try:
                server_reader, server_writer = await asyncio.wait_for(
                    asyncio.open_connection(route.host, route.port), timeout=5.0
                )
            except Exception as e:
                logger.error(f"Failed to connect to {route.host}:{route.port}: {e}")
                status_data = make_status_response(
                    handshake.protocol_version,
                    config.server_offline_motd % subdomain,
                    icon=config.server_offline_icon,
                )
                await async_write_packet(conn, StatusResponse(data=status_data))
                return

            server_conn = TCPAsyncConnection(reader=server_reader, writer=server_writer, timeout=10)
            await async_write_packet(server_conn, handshake)
            await pipe_streams(reader, writer, server_reader, server_writer)
            return

        if handshake.next_state == NextState.LOGIN:
            online = await route_manager.is_server_online(route.host, route.port)
            if not online:
                reason = ChatMessage({"text": config.server_offline_motd % subdomain})
                await async_write_packet(conn, LoginDisconnect(reason=reason))
                return

            try:
                server_reader, server_writer = await asyncio.wait_for(
                    asyncio.open_connection(route.host, route.port), timeout=5.0
                )
            except Exception as e:
                logger.error(f"Failed to connect to {route.host}:{route.port}: {e}")
                reason = ChatMessage({"text": "Failed to connect to backend server"})
                await async_write_packet(conn, LoginDisconnect(reason=reason))
                return

            server_conn = TCPAsyncConnection(reader=server_reader, writer=server_writer, timeout=30)
            await async_write_packet(server_conn, handshake)

            await route_manager.increment_connections(subdomain)
            try:
                await pipe_streams(reader, writer, server_reader, server_writer)
            finally:
                await route_manager.decrement_connections(subdomain)

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
