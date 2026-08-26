from __future__ import annotations

from mcproto.packets import generate_packet_map
from mcproto.packets.packet import PacketDirection, GameState

HANDSHAKE_SERVERBOUND_MAP = generate_packet_map(PacketDirection.SERVERBOUND, GameState.HANDSHAKING)
STATUS_CLIENTBOUND_MAP = generate_packet_map(PacketDirection.CLIENTBOUND, GameState.STATUS)
LOGIN_CLIENTBOUND_MAP = generate_packet_map(PacketDirection.CLIENTBOUND, GameState.LOGIN)
