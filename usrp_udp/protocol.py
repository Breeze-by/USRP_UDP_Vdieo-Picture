from __future__ import annotations

import struct
import zlib
from enum import IntEnum


MAGIC = b"UUDP"
VERSION = 1
HEADER_STRUCT = struct.Struct("!4sBBQIIHHI")
HEADER_SIZE = HEADER_STRUCT.size


class PacketKind(IntEnum):
    START = 1
    DATA = 2
    PARITY = 3
    END = 4


def build_packet(
    kind: PacketKind,
    session_id: int,
    payload: bytes = b"",
    field_a: int = 0,
    field_b: int = 0,
    field_c: int = 0,
) -> bytes:
    payload_crc = zlib.crc32(payload) & 0xFFFFFFFF
    header = HEADER_STRUCT.pack(
        MAGIC,
        VERSION,
        int(kind),
        session_id & 0xFFFFFFFFFFFFFFFF,
        field_a & 0xFFFFFFFF,
        field_b & 0xFFFFFFFF,
        field_c & 0xFFFF,
        len(payload) & 0xFFFF,
        payload_crc,
    )
    return header + payload


def parse_packet(packet: bytes) -> tuple[PacketKind, int, int, int, int, bytes]:
    if len(packet) < HEADER_SIZE:
        raise ValueError("packet too short")

    magic, version, kind_value, session_id, field_a, field_b, field_c, payload_len, payload_crc = (
        HEADER_STRUCT.unpack(packet[:HEADER_SIZE])
    )

    if magic != MAGIC:
        raise ValueError("bad magic")
    if version != VERSION:
        raise ValueError(f"unsupported version: {version}")
    if len(packet) != HEADER_SIZE + payload_len:
        raise ValueError("payload length mismatch")

    payload = packet[HEADER_SIZE:]
    actual_crc = zlib.crc32(payload) & 0xFFFFFFFF
    if actual_crc != payload_crc:
        raise ValueError("payload crc mismatch")

    try:
        kind = PacketKind(kind_value)
    except ValueError as exc:
        raise ValueError(f"unknown packet kind: {kind_value}") from exc

    return kind, session_id, field_a, field_b, field_c, payload
