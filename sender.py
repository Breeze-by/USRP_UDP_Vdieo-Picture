from __future__ import annotations

import argparse
import secrets
import socket
import time
from pathlib import Path

from usrp_udp.common import (
    build_metadata,
    compute_sha256,
    dumps_json,
    format_bytes,
    prepare_media,
)
from usrp_udp.protocol import PacketKind, build_packet


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Send an image or video to a USRP-facing UDP endpoint with framing and CRC."
    )
    parser.add_argument("--input", required=True, help="Input image/video path.")
    parser.add_argument("--host", required=True, help="Destination host or USRP IP.")
    parser.add_argument("--port", required=True, type=int, help="Destination UDP port.")
    parser.add_argument("--bind-host", default="", help="Optional local bind address.")
    parser.add_argument("--bind-port", type=int, default=0, help="Optional local bind port.")
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=1316,
        help="Payload bytes per UDP data frame. Keep <= 1400 to avoid IP fragmentation. 1316 matches 7 MPEG-TS packets.",
    )
    parser.add_argument(
        "--packet-interval-us",
        type=int,
        default=200,
        help="Delay between UDP packets in microseconds.",
    )
    parser.add_argument(
        "--control-repeat",
        type=int,
        default=3,
        help="How many times START/END control frames are repeated.",
    )
    parser.add_argument(
        "--control-interval-ms",
        type=int,
        default=50,
        help="Delay between repeated START/END control frames in milliseconds.",
    )
    parser.add_argument(
        "--streamable-ts",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="For video, try converting to MPEG-TS first so the receiver can play while receiving.",
    )
    parser.add_argument(
        "--socket-buffer-kb",
        type=int,
        default=1024,
        help="Socket send buffer in KiB.",
    )
    return parser.parse_args()


def send_control(
    sock: socket.socket,
    destination: tuple[str, int],
    kind: PacketKind,
    session_id: int,
    payload: bytes,
    repeat: int,
    interval_ms: int,
) -> None:
    packet = build_packet(kind, session_id, payload=payload)
    for attempt in range(repeat):
        sock.sendto(packet, destination)
        if attempt != repeat - 1 and interval_ms > 0:
            time.sleep(interval_ms / 1000.0)


def main() -> int:
    args = parse_args()
    input_path = Path(args.input).expanduser().resolve()
    if not input_path.is_file():
        raise FileNotFoundError(f"Input file not found: {input_path}")

    if args.chunk_size <= 0 or args.chunk_size > 1400:
        raise ValueError("--chunk-size must be between 1 and 1400")

    prepared, temp_dir = prepare_media(input_path, prefer_streamable_video=args.streamable_ts)
    transport_path = Path(prepared.transport_path)
    transport_size = transport_path.stat().st_size
    transport_sha256 = compute_sha256(transport_path)
    metadata = build_metadata(
        prepared=prepared,
        chunk_size=args.chunk_size,
        transport_size=transport_size,
        transport_sha256=transport_sha256,
    )

    session_id = secrets.randbits(64)
    destination = (args.host, args.port)
    packet_interval = args.packet_interval_us / 1_000_000.0
    total_chunks = metadata["total_chunks"]

    print(f"Input       : {input_path}")
    print(f"Transport   : {transport_path}")
    print(f"Media type  : {prepared.media_type}")
    print(f"Streamable  : {prepared.streamable}")
    print(f"Size        : {format_bytes(transport_size)}")
    print(f"Chunks      : {total_chunks}")
    if prepared.streamable and args.chunk_size % 188 != 0:
        print("Warning     : chunk-size is not aligned to 188-byte TS packets; 1316 is preferred for MPEG-TS streaming")
    print(f"Target      : {args.host}:{args.port}")
    print(f"Session ID  : {session_id:016x}")

    start_payload = dumps_json(metadata)
    end_payload = dumps_json(
        {
            "session_id": f"{session_id:016x}",
            "total_chunks": total_chunks,
            "transport_sha256": transport_sha256,
        }
    )

    start_time = time.perf_counter()
    sent_packets = 0
    sent_bytes = 0
    data_packets = 0
    control_packets = 0

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        if args.bind_host or args.bind_port:
            sock.bind((args.bind_host, args.bind_port))
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, args.socket_buffer_kb * 1024)

        send_control(
            sock,
            destination,
            PacketKind.START,
            session_id,
            start_payload,
            repeat=args.control_repeat,
            interval_ms=args.control_interval_ms,
        )
        sent_packets += args.control_repeat
        sent_bytes += len(start_payload) * args.control_repeat
        control_packets += args.control_repeat

        with transport_path.open("rb") as handle:
            chunk_id = 0
            last_report = time.perf_counter()
            last_report_bytes = 0
            last_report_packets = 0
            while True:
                chunk = handle.read(args.chunk_size)
                if not chunk:
                    break

                packet = build_packet(
                    PacketKind.DATA,
                    session_id,
                    payload=chunk,
                    field_a=chunk_id,
                )
                sock.sendto(packet, destination)
                sent_packets += 1
                sent_bytes += len(chunk)
                data_packets += 1
                chunk_id += 1
                if packet_interval > 0:
                    time.sleep(packet_interval)

                now = time.perf_counter()
                if now - last_report >= 1.0:
                    progress = min(chunk_id / total_chunks * 100.0, 100.0)
                    elapsed = now - start_time
                    interval = now - last_report
                    delta_bytes = sent_bytes - last_report_bytes
                    delta_packets = sent_packets - last_report_packets
                    rate = delta_bytes / interval if interval > 0 else 0.0
                    packet_rate = delta_packets / interval if interval > 0 else 0.0
                    remaining_chunks = total_chunks - chunk_id
                    chunk_rate = chunk_id / elapsed if elapsed > 0 else 0.0
                    eta = remaining_chunks / chunk_rate if chunk_rate > 0 else 0.0
                    print(
                        f"Progress    : {chunk_id}/{total_chunks} chunks "
                        f"({progress:.1f}%), tx {format_bytes(rate)}/s, "
                        f"{packet_rate:.0f} pkt/s, eta {eta:.2f}s"
                    )
                    last_report = now
                    last_report_bytes = sent_bytes
                    last_report_packets = sent_packets

        send_control(
            sock,
            destination,
            PacketKind.END,
            session_id,
            end_payload,
            repeat=args.control_repeat,
            interval_ms=args.control_interval_ms,
        )
        sent_packets += args.control_repeat
        sent_bytes += len(end_payload) * args.control_repeat
        control_packets += args.control_repeat
    finally:
        sock.close()
        if temp_dir is not None:
            temp_dir.cleanup()

    elapsed = time.perf_counter() - start_time
    rate = sent_bytes / elapsed if elapsed > 0 else 0.0
    packet_rate = sent_packets / elapsed if elapsed > 0 else 0.0
    print(
        f"Finished    : data {data_packets}, control {control_packets}, "
        f"total {sent_packets} packets in {elapsed:.2f}s"
    )
    print(f"Throughput  : {format_bytes(rate)}/s payload, {packet_rate:.0f} pkt/s")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
