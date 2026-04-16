from __future__ import annotations

import argparse
import secrets
import socket
import time
from collections import deque
from dataclasses import dataclass
from pathlib import Path

from usrp_udp.common import (
    EMPTY_SHA256,
    EntrySnapshot,
    build_entry_metadata,
    dumps_json,
    format_bytes,
    iter_directory_entries,
    stage_file_snapshot,
)
from usrp_udp.protocol import PacketKind, build_packet


def precise_sleep_until(deadline: float) -> None:
    while True:
        remaining = deadline - time.perf_counter()
        if remaining <= 0:
            return
        if remaining > 0.002:
            time.sleep(remaining - 0.001)
        elif remaining > 0.0002:
            time.sleep(0)


class PacketPacer:
    def __init__(self, target_rate_bytes_per_sec: float | None, packet_interval_sec: float) -> None:
        self.target_rate_bytes_per_sec = target_rate_bytes_per_sec
        self.packet_interval_sec = packet_interval_sec
        self.start_time = time.perf_counter()
        self.sent_payload_bytes = 0
        self.sent_packets = 0

    def pace_after_send(self, payload_bytes: int) -> None:
        if self.target_rate_bytes_per_sec and self.target_rate_bytes_per_sec > 0:
            self.sent_payload_bytes += payload_bytes
            deadline = self.start_time + (self.sent_payload_bytes / self.target_rate_bytes_per_sec)
            precise_sleep_until(deadline)
            return

        if self.packet_interval_sec > 0:
            self.sent_packets += 1
            deadline = self.start_time + (self.sent_packets * self.packet_interval_sec)
            precise_sleep_until(deadline)


@dataclass(slots=True)
class ObservedEntry:
    snapshot: EntrySnapshot
    stable_since: float


class DirectoryTracker:
    def __init__(self, root: Path, settle_sec: float) -> None:
        self.root = root
        self.settle_sec = settle_sec
        self.observed: dict[str, ObservedEntry] = {}
        self.sent_signatures: dict[str, tuple[str, int, int]] = {}
        self.queued_signatures: dict[str, tuple[str, int, int]] = {}

    def poll(self) -> list[EntrySnapshot]:
        now = time.monotonic()
        current: dict[str, ObservedEntry] = {}
        for snapshot in iter_directory_entries(self.root):
            previous = self.observed.get(snapshot.relative_path)
            stable_since = now
            if previous is not None and previous.snapshot.signature == snapshot.signature:
                stable_since = previous.stable_since
            current[snapshot.relative_path] = ObservedEntry(snapshot=snapshot, stable_since=stable_since)

        stale_queue_keys = [
            relative_path
            for relative_path, signature in self.queued_signatures.items()
            if relative_path not in current or current[relative_path].snapshot.signature != signature
        ]
        for relative_path in stale_queue_keys:
            self.queued_signatures.pop(relative_path, None)

        self.observed = current

        ready: list[EntrySnapshot] = []
        for relative_path in sorted(current):
            observed = current[relative_path]
            if now - observed.stable_since < self.settle_sec:
                continue
            signature = observed.snapshot.signature
            if self.sent_signatures.get(relative_path) == signature:
                continue
            if self.queued_signatures.get(relative_path) == signature:
                continue
            ready.append(observed.snapshot)
            self.queued_signatures[relative_path] = signature
        return ready

    def mark_dequeued(self, snapshot: EntrySnapshot) -> None:
        if self.queued_signatures.get(snapshot.relative_path) == snapshot.signature:
            self.queued_signatures.pop(snapshot.relative_path, None)

    def mark_sent(self, snapshot: EntrySnapshot) -> None:
        self.sent_signatures[snapshot.relative_path] = snapshot.signature

    def is_current(self, snapshot: EntrySnapshot) -> bool:
        current = self.observed.get(snapshot.relative_path)
        return current is not None and current.snapshot.signature == snapshot.signature

    def count_pending(self) -> tuple[int, int]:
        now = time.monotonic()
        ready_unsent = 0
        unsettled = 0
        for relative_path, observed in self.observed.items():
            if now - observed.stable_since < self.settle_sec:
                unsettled += 1
                continue
            signature = observed.snapshot.signature
            if self.sent_signatures.get(relative_path) == signature:
                continue
            if self.queued_signatures.get(relative_path) == signature:
                continue
            ready_unsent += 1
        return ready_unsent, unsettled


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Continuously send all files and directories under a shared folder as per-entry UDP sessions."
    )
    parser.add_argument("--input-dir", default="shared", help="Directory to watch and send recursively.")
    parser.add_argument("--host", required=True, help="Destination host or USRP IP.")
    parser.add_argument("--port", required=True, type=int, help="Destination UDP port.")
    parser.add_argument("--bind-host", default="", help="Optional local bind address.")
    parser.add_argument("--bind-port", type=int, default=0, help="Optional local bind port.")
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=1316,
        help="Payload bytes per UDP data frame. Keep <= 1400 to avoid IP fragmentation.",
    )
    parser.add_argument(
        "--packet-interval-us",
        type=int,
        default=200,
        help="Delay between UDP packets in microseconds. Ignored when --target-rate-mbps is set.",
    )
    parser.add_argument(
        "--target-rate-mbps",
        type=float,
        default=None,
        help="Target payload throughput in MB/s. Example: 8.0 means about 8 MB/s payload.",
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
        "--socket-buffer-kb",
        type=int,
        default=8192,
        help="Socket send buffer in KiB.",
    )
    parser.add_argument(
        "--scan-interval-ms",
        type=int,
        default=500,
        help="How often to rescan the shared directory for new or updated entries.",
    )
    parser.add_argument(
        "--settle-ms",
        type=int,
        default=500,
        help="Entry must stay unchanged for this long before it is eligible to send.",
    )
    parser.add_argument(
        "--scan-once",
        action="store_true",
        help="Send the current snapshot of the directory tree and exit after everything stable has been sent.",
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


def send_snapshot(
    sock: socket.socket,
    destination: tuple[str, int],
    snapshot: EntrySnapshot,
    sequence_number: int,
    args: argparse.Namespace,
    target_rate_bytes_per_sec: float | None,
    packet_interval_sec: float,
) -> dict | None:
    staged = None
    try:
        if snapshot.entry_type == "file":
            staged = stage_file_snapshot(snapshot)
            if staged is None:
                print(f"Deferred     : {snapshot.relative_path} changed while staging, waiting for the next stable version")
                return None
            content_size = staged.size
            content_sha256 = staged.sha256
            source_path = staged.staged_path
        else:
            content_size = 0
            content_sha256 = EMPTY_SHA256
            source_path = None

        metadata = build_entry_metadata(
            snapshot=snapshot,
            chunk_size=args.chunk_size,
            content_sha256=content_sha256,
            sequence_number=sequence_number,
        )
        total_chunks = metadata["total_chunks"]
        session_id = secrets.randbits(64)

        print(
            f"Sending     : seq {sequence_number}, session {session_id:016x}, "
            f"{snapshot.entry_type} {snapshot.relative_path}"
        )
        print(f"Size        : {format_bytes(content_size)}")
        print(f"Chunks      : {total_chunks}")
        print(f"Target      : {destination[0]}:{destination[1]}")

        start_payload = dumps_json(metadata)
        end_payload = dumps_json(
            {
                "session_id": f"{session_id:016x}",
                "sequence_number": sequence_number,
                "relative_path": snapshot.relative_path,
                "total_chunks": total_chunks,
                "content_sha256": content_sha256,
            }
        )

        sent_packets = 0
        sent_bytes = 0
        data_packets = 0
        control_packets = 0
        start_time = time.perf_counter()
        pacer = PacketPacer(
            target_rate_bytes_per_sec=target_rate_bytes_per_sec,
            packet_interval_sec=packet_interval_sec,
        )

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

        data_phase_start = time.perf_counter()
        if source_path is not None:
            with source_path.open("rb") as handle:
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
                    pacer.pace_after_send(len(chunk))

                    now = time.perf_counter()
                    if now - last_report >= 1.0 and total_chunks > 0:
                        progress = min(chunk_id / total_chunks * 100.0, 100.0)
                        interval = now - last_report
                        delta_bytes = sent_bytes - last_report_bytes
                        delta_packets = sent_packets - last_report_packets
                        rate = delta_bytes / interval if interval > 0 else 0.0
                        packet_rate = delta_packets / interval if interval > 0 else 0.0
                        print(
                            f"Progress    : seq {sequence_number} "
                            f"{chunk_id}/{total_chunks} chunks ({progress:.1f}%), "
                            f"tx {format_bytes(rate)}/s, {packet_rate:.0f} pkt/s"
                        )
                        last_report = now
                        last_report_bytes = sent_bytes
                        last_report_packets = sent_packets
        data_phase_end = time.perf_counter()

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

        elapsed = time.perf_counter() - start_time
        session_rate = sent_bytes / elapsed if elapsed > 0 else 0.0
        data_elapsed = data_phase_end - data_phase_start
        data_rate = content_size / data_elapsed if data_elapsed > 0 else 0.0

        print(
            f"Finished    : seq {sequence_number}, data {data_packets}, control {control_packets}, "
            f"total {sent_packets} packets in {elapsed:.2f}s"
        )
        print(f"Throughput  : {format_bytes(session_rate)}/s overall, {format_bytes(data_rate)}/s during DATA")

        return {
            "entry_type": snapshot.entry_type,
            "relative_path": snapshot.relative_path,
            "payload_bytes": content_size,
            "sent_packets": sent_packets,
        }
    finally:
        if staged is not None:
            staged.cleanup()


def main() -> int:
    args = parse_args()
    input_dir = Path(args.input_dir).expanduser().resolve()
    if not input_dir.is_dir():
        raise FileNotFoundError(f"Input directory not found: {input_dir}")
    if args.chunk_size <= 0 or args.chunk_size > 1400:
        raise ValueError("--chunk-size must be between 1 and 1400")
    if args.target_rate_mbps is not None and args.target_rate_mbps <= 0:
        raise ValueError("--target-rate-mbps must be > 0")

    destination = (args.host, args.port)
    packet_interval_sec = args.packet_interval_us / 1_000_000.0
    target_rate_bytes_per_sec = args.target_rate_mbps * 1_000_000.0 if args.target_rate_mbps is not None else None
    tracker = DirectoryTracker(root=input_dir, settle_sec=max(args.settle_ms, 0) / 1000.0)
    pending: deque[EntrySnapshot] = deque()
    sequence_number = 1

    total_entries = 0
    total_files = 0
    total_directories = 0
    total_payload_bytes = 0
    total_packets = 0

    print(f"Input dir   : {input_dir}")
    print(f"Target      : {args.host}:{args.port}")
    if target_rate_bytes_per_sec is not None:
        print(f"Target rate : {args.target_rate_mbps:.2f} MB/s payload")
    else:
        print(f"Packet gap  : {args.packet_interval_us} us")
    print(f"Scan mode   : {'single snapshot' if args.scan_once else 'continuous watch'}")
    print(f"Settle time : {args.settle_ms} ms")
    print(f"Scan every  : {args.scan_interval_ms} ms")

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        if args.bind_host or args.bind_port:
            sock.bind((args.bind_host, args.bind_port))
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, args.socket_buffer_kb * 1024)

        while True:
            for snapshot in tracker.poll():
                pending.append(snapshot)

            if pending:
                snapshot = pending.popleft()
                tracker.mark_dequeued(snapshot)
                if not tracker.is_current(snapshot):
                    continue

                current_path = Path(snapshot.absolute_path)
                if snapshot.entry_type == "file" and not current_path.is_file():
                    continue
                if snapshot.entry_type == "directory" and not current_path.is_dir():
                    continue

                result = send_snapshot(
                    sock=sock,
                    destination=destination,
                    snapshot=snapshot,
                    sequence_number=sequence_number,
                    args=args,
                    target_rate_bytes_per_sec=target_rate_bytes_per_sec,
                    packet_interval_sec=packet_interval_sec,
                )
                if result is None:
                    continue

                tracker.mark_sent(snapshot)
                sequence_number += 1
                total_entries += 1
                total_payload_bytes += result["payload_bytes"]
                total_packets += result["sent_packets"]
                if result["entry_type"] == "file":
                    total_files += 1
                else:
                    total_directories += 1
                continue

            ready_unsent, unsettled = tracker.count_pending()
            if args.scan_once and ready_unsent == 0 and unsettled == 0:
                break

            time.sleep(max(args.scan_interval_ms, 1) / 1000.0)
    except KeyboardInterrupt:
        print("Stopped     : sender interrupted by user")
        return 130
    finally:
        sock.close()

    print(
        f"Summary     : sent {total_entries} entries "
        f"({total_files} files, {total_directories} directories), "
        f"{format_bytes(total_payload_bytes)} payload, {total_packets} packets"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
