from __future__ import annotations

import argparse
import os
import queue
import socket
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import BinaryIO

from usrp_udp.common import (
    compute_sha256,
    format_bytes,
    infer_chunk_length,
    loads_json,
    resolve_output_path,
)
from usrp_udp.protocol import PacketKind, parse_packet

RECENT_SESSION_TTL = 10.0
RECEIVE_DIVIDER = "=" * 78


def apply_modified_time(path: Path, modified_ns: int) -> None:
    if modified_ns <= 0:
        return
    try:
        os.utime(path, ns=(modified_ns, modified_ns))
    except OSError:
        pass


@dataclass(slots=True)
class SessionState:
    session_id: int
    sequence_number: int
    entry_type: str
    relative_path: str
    final_path: Path
    partial_path: Path | None
    chunk_size: int
    total_chunks: int
    content_size: int
    content_sha256: str
    modified_ns: int
    output_fp: BinaryIO | None
    received: bytearray
    chunks_received: int = 0
    received_payload_bytes: int = 0
    end_received: bool = False
    end_received_at: float | None = None
    last_activity: float = field(default_factory=time.monotonic)
    last_flush: float = field(default_factory=time.monotonic)
    last_report_rx_bytes: int = 0

    def handle_data(self, chunk_id: int, payload: bytes) -> None:
        if self.entry_type != "file":
            return
        if chunk_id < 0 or chunk_id >= self.total_chunks:
            return

        self.last_activity = time.monotonic()
        if self.received[chunk_id]:
            return

        payload = self._normalize_chunk(chunk_id, payload)
        if self.output_fp is None:
            raise ValueError("file session has no writable handle")
        self.output_fp.seek(chunk_id * self.chunk_size)
        self.output_fp.write(payload)
        self.received[chunk_id] = 1
        self.chunks_received += 1
        self.received_payload_bytes += len(payload)
        self.maybe_flush()

    def mark_end(self) -> None:
        now = time.monotonic()
        self.end_received = True
        self.end_received_at = now
        self.last_activity = now

    def maybe_flush(self) -> None:
        if self.output_fp is None:
            return
        now = time.monotonic()
        if now - self.last_flush >= 0.5:
            self.output_fp.flush()
            self.last_flush = now

    def is_complete(self) -> bool:
        return self.chunks_received == self.total_chunks

    def missing_chunks(self) -> int:
        return self.total_chunks - self.chunks_received

    def progress_line(self, interval: float) -> str:
        rx_delta = self.received_payload_bytes - self.last_report_rx_bytes
        self.last_report_rx_bytes = self.received_payload_bytes
        progress = self.chunks_received / self.total_chunks * 100.0 if self.total_chunks else 100.0
        return (
            f"progress RX {self.sequence_number:04d} | "
            f"{self.chunks_received}/{self.total_chunks} ({progress:.1f}%) "
            f"| rx={format_bytes(rx_delta / interval if interval > 0 else 0.0)}/s"
        )

    def finalize(self) -> Path:
        if self.entry_type == "directory":
            self.final_path.mkdir(parents=True, exist_ok=True)
            apply_modified_time(self.final_path, self.modified_ns)
            return self.final_path

        if self.output_fp is not None:
            self.output_fp.flush()
            self.output_fp.close()
            self.output_fp = None
        if self.partial_path is None:
            raise ValueError("file session has no partial path")

        actual_sha256 = compute_sha256(self.partial_path)
        if actual_sha256 != self.content_sha256:
            raise ValueError(
                f"SHA256 mismatch for {self.relative_path}: "
                f"expected {self.content_sha256}, got {actual_sha256}"
            )

        self.final_path.parent.mkdir(parents=True, exist_ok=True)
        os.replace(self.partial_path, self.final_path)
        apply_modified_time(self.final_path, self.modified_ns)
        return self.final_path

    def drop_partial(self) -> None:
        if self.output_fp is not None:
            self.output_fp.close()
            self.output_fp = None
        if self.partial_path is not None:
            try:
                self.partial_path.unlink()
            except FileNotFoundError:
                pass

    def _normalize_chunk(self, chunk_id: int, payload: bytes) -> bytes:
        expected = infer_chunk_length(chunk_id, self.total_chunks, self.content_size, self.chunk_size)
        if len(payload) == expected:
            return payload
        if len(payload) > expected:
            return payload[:expected]
        return payload + b"\x00" * (expected - len(payload))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Receive directory-entry UDP sessions and restore files under the output directory."
    )
    parser.add_argument("--bind-host", default="0.0.0.0", help="Local host/IP to bind.")
    parser.add_argument("--port", required=True, type=int, help="Local UDP port to listen on.")
    parser.add_argument("--output-dir", default="received", help="Directory where restored files will be written.")
    parser.add_argument(
        "--idle-timeout",
        type=float,
        default=5.0,
        help="Seconds without packets for a session before it is reported as incomplete and dropped.",
    )
    parser.add_argument(
        "--post-end-timeout-ms",
        type=int,
        default=1000,
        help="After END arrives, wait this long for any missing chunks before dropping the file.",
    )
    parser.add_argument("--socket-buffer-kb", type=int, default=32768, help="Socket receive buffer in KiB.")
    parser.add_argument(
        "--packet-queue-size",
        type=int,
        default=8192,
        help="Number of UDP packets buffered in userspace before processing.",
    )
    parser.add_argument(
        "--exit-when-idle",
        type=float,
        default=None,
        help="For tests: exit once at least one packet has been seen, there are no active sessions, and the receiver stays globally idle for this many seconds.",
    )
    return parser.parse_args()


def create_session(output_dir: Path, session_id: int, metadata: dict) -> SessionState:
    protocol = metadata.get("protocol")
    if protocol != "usrp_udp_directory_v2":
        raise ValueError(f"unsupported metadata protocol: {protocol!r}")

    entry_type = metadata.get("entry_type")
    if entry_type not in {"file", "directory"}:
        raise ValueError(f"unsupported entry type: {entry_type!r}")

    relative_path = metadata["relative_path"]
    final_path = resolve_output_path(output_dir, relative_path)
    chunk_size = int(metadata["chunk_size"])
    total_chunks = int(metadata["total_chunks"])
    content_size = int(metadata["content_size"])
    if chunk_size <= 0:
        raise ValueError("chunk_size must be > 0")
    if total_chunks < 0:
        raise ValueError("total_chunks must be >= 0")
    if content_size < 0:
        raise ValueError("content_size must be >= 0")
    if entry_type == "directory" and (content_size != 0 or total_chunks != 0):
        raise ValueError("directory entries must have zero size and zero chunks")
    if entry_type == "file" and total_chunks == 0 and content_size != 0:
        raise ValueError("non-empty file entries must have at least one chunk")

    partial_path: Path | None = None
    output_fp: BinaryIO | None = None
    if entry_type == "file":
        final_path.parent.mkdir(parents=True, exist_ok=True)
        partial_path = final_path.with_name(f"{final_path.name}.part.{session_id:016x}")
        try:
            partial_path.unlink()
        except FileNotFoundError:
            pass
        output_fp = partial_path.open("wb+")

    return SessionState(
        session_id=session_id,
        sequence_number=int(metadata["sequence_number"]),
        entry_type=entry_type,
        relative_path=relative_path,
        final_path=final_path,
        partial_path=partial_path,
        chunk_size=chunk_size,
        total_chunks=total_chunks,
        content_size=content_size,
        content_sha256=metadata["content_sha256"],
        modified_ns=int(metadata.get("modified_ns", 0)),
        output_fp=output_fp,
        received=bytearray(total_chunks),
    )


def socket_reader(
    sock: socket.socket,
    packet_queue: queue.Queue[tuple[bytes, tuple[str, int]]],
    stop_event: threading.Event,
) -> None:
    while not stop_event.is_set():
        try:
            packet, peer = sock.recvfrom(65535)
        except socket.timeout:
            continue
        except OSError:
            break

        while not stop_event.is_set():
            try:
                packet_queue.put((packet, peer), timeout=0.1)
                break
            except queue.Full:
                continue


def print_receiver_header(
    bind_host: str,
    port: int,
    output_dir: Path,
    requested_socket_buffer_kb: int,
    actual_socket_buffer: int,
    packet_queue_size: int,
) -> None:
    print(f"Listen      : {bind_host}:{port}")
    print(f"Output      : {output_dir}")
    print(
        f"Buffer      : requested {format_bytes(requested_socket_buffer_kb * 1024)}, "
        f"actual {format_bytes(actual_socket_buffer)}, queue {packet_queue_size} packets"
    )


def print_session_start(state: SessionState, peer: tuple[str, int]) -> None:
    print()
    print(RECEIVE_DIVIDER)
    print(f"RX {state.sequence_number:04d} | {state.entry_type} | {state.relative_path}")
    print(
        f"session={state.session_id:016x} from={peer[0]}:{peer[1]} "
        f"size={format_bytes(state.content_size)} chunks={state.total_chunks}"
    )


def complete_session(state: SessionState) -> Path:
    restored_path = state.finalize()
    print(f"done   RX {state.sequence_number:04d} | restored={restored_path}")
    print(
        f"stats  RX {state.sequence_number:04d} | "
        f"chunks={state.chunks_received}/{state.total_chunks} | size={format_bytes(state.content_size)}"
    )
    return restored_path


def drop_session(state: SessionState, reason: str) -> None:
    missing = state.missing_chunks()
    state.drop_partial()
    print(f"drop   RX {state.sequence_number:04d} | missing={missing}/{state.total_chunks} | reason={reason}")


def main() -> int:
    args = parse_args()
    output_dir = Path(args.output_dir).expanduser().resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        sock.bind((args.bind_host, args.port))
    except OSError as exc:
        sock.close()
        if getattr(exc, "winerror", None) == 10048:
            raise SystemExit(
                f"UDP port {args.port} is already in use on {args.bind_host}. "
                "Choose another port or stop the process currently holding it."
            ) from exc
        raise

    sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, args.socket_buffer_kb * 1024)
    sock.settimeout(0.5)
    actual_socket_buffer = sock.getsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF)

    packet_queue: queue.Queue[tuple[bytes, tuple[str, int]]] = queue.Queue(maxsize=max(args.packet_queue_size, 1))
    stop_event = threading.Event()
    reader_thread = threading.Thread(
        target=socket_reader,
        args=(sock, packet_queue, stop_event),
        name="udp-recv",
        daemon=True,
    )
    reader_thread.start()

    sessions: dict[int, SessionState] = {}
    recent_sessions: dict[int, float] = {}
    last_report = time.monotonic()
    last_packet_at = last_report
    saw_packets = False
    had_failures = False

    print_receiver_header(
        bind_host=args.bind_host,
        port=args.port,
        output_dir=output_dir,
        requested_socket_buffer_kb=args.socket_buffer_kb,
        actual_socket_buffer=actual_socket_buffer,
        packet_queue_size=args.packet_queue_size,
    )

    try:
        while True:
            try:
                packet, peer = packet_queue.get(timeout=0.1)
            except queue.Empty:
                packet = None
                peer = ("", 0)

            now = time.monotonic()
            if packet is not None:
                saw_packets = True
                last_packet_at = now
                try:
                    kind, session_id, field_a, field_b, field_c, payload = parse_packet(packet)
                except ValueError:
                    continue

                recent_sessions = {
                    completed_session_id: seen_at
                    for completed_session_id, seen_at in recent_sessions.items()
                    if now - seen_at <= RECENT_SESSION_TTL
                }

                if session_id in recent_sessions:
                    continue

                if kind == PacketKind.START:
                    if session_id in sessions:
                        continue
                    try:
                        metadata = loads_json(payload)
                        state = create_session(output_dir=output_dir, session_id=session_id, metadata=metadata)
                    except Exception as exc:
                        print(f"reject RX ???? | session={session_id:016x} | reason={exc}")
                        had_failures = True
                        recent_sessions[session_id] = now
                        continue

                    sessions[session_id] = state
                    print_session_start(state, peer)
                    continue

                state = sessions.get(session_id)
                if state is None:
                    continue

                if kind == PacketKind.DATA:
                    state.handle_data(field_a, payload)
                elif kind == PacketKind.END:
                    state.mark_end()
                    if state.is_complete():
                        try:
                            complete_session(state)
                        except Exception as exc:
                            drop_session(state, reason=f"verification failed ({exc})")
                            had_failures = True
                        sessions.pop(session_id, None)
                        recent_sessions[session_id] = now

            expired: list[int] = []
            for session_id, state in list(sessions.items()):
                state.maybe_flush()
                if state.end_received and state.is_complete():
                    try:
                        complete_session(state)
                    except Exception as exc:
                        drop_session(state, reason=f"verification failed ({exc})")
                        had_failures = True
                    expired.append(session_id)
                    recent_sessions[session_id] = now
                    continue

                if state.end_received and now - state.last_activity >= max(args.post_end_timeout_ms, 0) / 1000.0:
                    drop_session(state, reason=f"END received and no more data arrived for {args.post_end_timeout_ms} ms")
                    expired.append(session_id)
                    recent_sessions[session_id] = now
                    had_failures = True
                    continue

                if now - state.last_activity >= args.idle_timeout:
                    drop_session(state, reason=f"timed out after {args.idle_timeout:.1f}s")
                    expired.append(session_id)
                    recent_sessions[session_id] = now
                    had_failures = True

            for session_id in expired:
                sessions.pop(session_id, None)

            if now - last_report >= 1.0:
                interval = now - last_report
                for state in sessions.values():
                    print(state.progress_line(interval))
                last_report = now

            if (
                args.exit_when_idle is not None
                and saw_packets
                and not sessions
                and now - last_packet_at >= args.exit_when_idle
            ):
                return 1 if had_failures else 0
    except KeyboardInterrupt:
        print()
        print("Stopped     : receiver interrupted by user")
        return 130
    finally:
        stop_event.set()
        try:
            sock.close()
        except OSError:
            pass
        reader_thread.join(timeout=1.0)
        for state in sessions.values():
            state.drop_partial()


if __name__ == "__main__":
    raise SystemExit(main())
