from __future__ import annotations

import argparse
import json
import queue
import socket
import subprocess
import threading
import time
from collections import deque
from dataclasses import dataclass, field
from pathlib import Path
from typing import BinaryIO

import av
import cv2

from usrp_udp.common import (
    compute_sha256,
    format_bytes,
    get_ffmpeg_executable,
    infer_chunk_length,
    loads_json,
    safe_filename,
)
from usrp_udp.protocol import PacketKind, parse_packet


TS_PACKET_SIZE = 188


class BlockingByteStream:
    def __init__(self) -> None:
        self._buffer = bytearray()
        self._closed = False
        self._condition = threading.Condition()

    def write(self, payload: bytes) -> None:
        if not payload:
            return
        with self._condition:
            if self._closed:
                return
            self._buffer.extend(payload)
            self._condition.notify_all()

    def read(self, size: int = -1) -> bytes:
        with self._condition:
            while not self._buffer and not self._closed:
                self._condition.wait(timeout=0.1)
            if not self._buffer:
                return b""
            if size is None or size < 0 or size > len(self._buffer):
                size = len(self._buffer)
            payload = bytes(self._buffer[:size])
            del self._buffer[:size]
            return payload

    def buffered_bytes(self) -> int:
        with self._condition:
            return len(self._buffer)

    def close(self) -> None:
        with self._condition:
            self._closed = True
            self._condition.notify_all()

    def readable(self) -> bool:
        return True

    def writable(self) -> bool:
        return False

    def seekable(self) -> bool:
        return False

    def seek(self, offset: int, whence: int = 0) -> int:
        raise OSError("stream is not seekable")

    def tell(self) -> int:
        return 0


@dataclass(slots=True)
class LivePlayer:
    session_id: int
    transport_name: str
    startup_threshold: int
    replay_buffer_limit: int
    restart_delay: float
    no_display: bool = False
    playback_thread: threading.Thread | None = None
    byte_stream: BlockingByteStream | None = None
    stop_signal: threading.Event | None = None
    pending_chunks: deque[bytes] = field(default_factory=deque)
    replay_history: deque[bytes] = field(default_factory=deque)
    events: queue.SimpleQueue[str] = field(default_factory=queue.SimpleQueue)
    lock: threading.Lock = field(default_factory=threading.Lock)
    pending_bytes: int = 0
    replay_history_bytes: int = 0
    restart_pending_reason: str | None = None
    last_exit_reason: str | None = None
    restart_count: int = 0
    stop_requested: bool = False
    last_start_attempt: float = 0.0

    def feed(self, payload: bytes) -> None:
        if not payload:
            return

        now = time.monotonic()
        byte_stream: BlockingByteStream | None = None
        with self.lock:
            self._service_locked(now)
            self._append_replay_locked(payload)
            if self.playback_thread is None:
                self.pending_chunks.append(payload)
                self.pending_bytes += len(payload)
                self._maybe_start_locked(now)
                return
            byte_stream = self.byte_stream

        if byte_stream is not None:
            byte_stream.write(payload)

    def service(self) -> list[str]:
        with self.lock:
            now = time.monotonic()
            self._service_locked(now)
            self._maybe_start_locked(now)

        messages: list[str] = []
        while True:
            try:
                messages.append(self.events.get_nowait())
            except queue.Empty:
                break
        return messages

    def buffered_bytes(self) -> int:
        with self.lock:
            byte_stream = self.byte_stream
            return self.pending_bytes + (byte_stream.buffered_bytes() if byte_stream is not None else 0)

    def is_running(self) -> bool:
        with self.lock:
            return self.playback_thread is not None and self.playback_thread.is_alive()

    def force_start(self) -> bool:
        with self.lock:
            if self.playback_thread is not None or self.pending_bytes <= 0:
                return False
            self._maybe_start_locked(time.monotonic(), force=True)
            return self.playback_thread is not None

    def finish(self) -> None:
        with self.lock:
            self.stop_requested = True
            now = time.monotonic()
            self._service_locked(now)
            if self.playback_thread is None and self.pending_bytes > 0:
                self._maybe_start_locked(now, force=True)
            byte_stream = self.byte_stream
            playback_thread = self.playback_thread

        if byte_stream is not None:
            byte_stream.close()
        if playback_thread is not None:
            playback_thread.join(timeout=5.0)

    def terminate(self) -> None:
        with self.lock:
            self.stop_requested = True
            stop_signal = self.stop_signal
            byte_stream = self.byte_stream
            playback_thread = self.playback_thread
            self.stop_signal = None
            self.byte_stream = None
            self.playback_thread = None

        if stop_signal is not None:
            stop_signal.set()
        if byte_stream is not None:
            byte_stream.close()
        if playback_thread is not None:
            playback_thread.join(timeout=1.0)

    def _append_replay_locked(self, payload: bytes) -> None:
        self.replay_history.append(payload)
        self.replay_history_bytes += len(payload)
        while self.replay_history and self.replay_history_bytes > self.replay_buffer_limit:
            dropped = self.replay_history.popleft()
            self.replay_history_bytes -= len(dropped)

    def _service_locked(self, now: float) -> None:
        if self.playback_thread is None or self.playback_thread.is_alive():
            return

        reason = self.last_exit_reason or "player thread exited"
        self.stop_signal = None
        self.byte_stream = None
        self.playback_thread = None
        if self.stop_requested:
            return

        self.pending_chunks = deque(self.replay_history)
        self.pending_bytes = self.replay_history_bytes
        self.restart_pending_reason = reason
        self.events.put(
            f"Playback    : session {self.session_id:016x} player stopped ({reason}), "
            f"rebuffering {format_bytes(self.pending_bytes)}"
        )
        self._maybe_start_locked(now)

    def _maybe_start_locked(self, now: float, force: bool = False) -> None:
        if self.playback_thread is not None or (self.stop_requested and not force):
            return
        if not force and self.pending_bytes < self.startup_threshold:
            return
        if now - self.last_start_attempt < self.restart_delay:
            return

        self.last_start_attempt = now
        byte_stream = BlockingByteStream()
        stop_signal = threading.Event()
        playback_thread = threading.Thread(
            target=self._playback_loop,
            args=(byte_stream, stop_signal),
            name=f"live-player-{self.session_id:016x}",
            daemon=True,
        )
        playback_thread.start()

        self.last_exit_reason = None
        self.byte_stream = byte_stream
        self.stop_signal = stop_signal
        self.playback_thread = playback_thread

        startup_bytes = self.pending_bytes
        if not force:
            startup_bytes = max(self.startup_threshold, self.pending_bytes)

        if self.restart_pending_reason is None:
            self.events.put(
                f"Playback    : session {self.session_id:016x} started, "
                f"startup buffer {format_bytes(startup_bytes)}"
            )
        else:
            self.restart_count += 1
            self.events.put(
                f"Playback    : session {self.session_id:016x} restarted "
                f"(count={self.restart_count}), replay {format_bytes(self.pending_bytes)}"
            )
            self.restart_pending_reason = None

        while self.pending_chunks:
            payload = self.pending_chunks.popleft()
            byte_stream.write(payload)
        self.pending_bytes = 0

    def _playback_loop(self, byte_stream: BlockingByteStream, stop_signal: threading.Event) -> None:
        window_name = f"USRP UDP Live {self.transport_name}"
        exit_reason = "stream ended"
        try:
            container = av.open(byte_stream, mode="r", format="mpegts")
        except Exception as exc:
            exit_reason = f"decoder init failed ({exc})"
            with self.lock:
                self.last_exit_reason = exit_reason
            return

        try:
            video_stream = next((stream for stream in container.streams if stream.type == "video"), None)
            if video_stream is None:
                exit_reason = "no video stream"
                return

            frame_interval = 0.0
            if video_stream.average_rate is not None:
                try:
                    frame_interval = float(video_stream.average_rate.denominator / video_stream.average_rate.numerator)
                except ZeroDivisionError:
                    frame_interval = 0.0

            if not self.no_display:
                cv2.namedWindow(window_name, cv2.WINDOW_NORMAL)

            start_wall: float | None = None
            start_media: float | None = None
            last_media = 0.0

            for frame in container.decode(video=0):
                if stop_signal.is_set():
                    exit_reason = "stopped"
                    break

                frame_time = frame.time
                if frame_time is None:
                    if start_media is None:
                        frame_time = 0.0
                    else:
                        frame_time = last_media + frame_interval
                media_time = max(float(frame_time), 0.0)

                if start_wall is None:
                    start_wall = time.perf_counter()
                    start_media = media_time

                target_time = start_wall + (media_time - start_media)
                while True:
                    delay = target_time - time.perf_counter()
                    if delay <= 0:
                        break
                    if stop_signal.is_set():
                        exit_reason = "stopped"
                        break
                    time.sleep(min(delay, 0.01))
                if stop_signal.is_set():
                    exit_reason = "stopped"
                    break

                last_media = media_time
                if self.no_display:
                    continue

                image = frame.to_ndarray(format="bgr24")
                cv2.imshow(window_name, image)
                cv2.waitKey(1)
                visible = cv2.getWindowProperty(window_name, cv2.WND_PROP_VISIBLE)
                if visible < 1:
                    exit_reason = "window closed by user"
                    break
        except Exception as exc:
            exit_reason = f"decoder error ({exc})"
        finally:
            try:
                container.close()
            except Exception:
                pass
            if not self.no_display:
                try:
                    cv2.destroyWindow(window_name)
                    cv2.waitKey(1)
                except Exception:
                    pass
            with self.lock:
                self.last_exit_reason = exit_reason


@dataclass(slots=True)
class SessionState:
    session_id: int
    metadata: dict
    session_dir: Path
    transport_path: Path
    manifest_path: Path
    chunk_size: int
    total_chunks: int
    transport_size: int
    transport_sha256: str
    transport_name: str
    original_name: str
    transport_fp: BinaryIO
    received: bytearray
    live_player: LivePlayer | None = None
    playback_disabled_reason: str | None = None
    playback_pending: dict[int, bytes] = field(default_factory=dict)
    chunks_received: int = 0
    received_payload_bytes: int = 0
    playback_bytes_emitted: int = 0
    playback_pending_bytes: int = 0
    next_playback_chunk: int = 0
    end_received: bool = False
    playback_min_start_bytes: int = 256 * 1024
    playback_start_timeout: float = 1.0
    last_activity: float = field(default_factory=time.monotonic)
    last_flush: float = field(default_factory=time.monotonic)
    last_report_rx_bytes: int = 0
    last_report_feed_bytes: int = 0
    last_ready_advance: float = field(default_factory=time.monotonic)
    last_wait_report_chunk: int = -1
    last_wait_report_at: float = 0.0

    def handle_data(self, chunk_id: int, payload: bytes) -> None:
        if chunk_id >= self.total_chunks:
            return
        self.last_activity = time.monotonic()
        if self.received[chunk_id]:
            return

        self._write_chunk(chunk_id, payload)

    def mark_end(self) -> None:
        self.end_received = True
        self.last_activity = time.monotonic()

    def maybe_flush(self) -> None:
        now = time.monotonic()
        if now - self.last_flush >= 0.5:
            self.transport_fp.flush()
            self.last_flush = now

    def is_complete(self) -> bool:
        return self.chunks_received == self.total_chunks

    def service_live_playback(self) -> list[str]:
        if self.live_player is None:
            return []
        messages = self.live_player.service()
        self._drain_live_stream()
        messages.extend(self.live_player.service())
        messages.extend(self._service_playback_waiting())
        return messages

    def progress_line(self, interval: float) -> str:
        rx_delta = self.received_payload_bytes - self.last_report_rx_bytes
        feed_delta = self.playback_bytes_emitted - self.last_report_feed_bytes
        self.last_report_rx_bytes = self.received_payload_bytes
        self.last_report_feed_bytes = self.playback_bytes_emitted

        progress = self.chunks_received / self.total_chunks * 100.0 if self.total_chunks else 100.0
        line = (
            f"Progress    : session {self.session_id:016x} "
            f"{self.chunks_received}/{self.total_chunks} chunks ({progress:.1f}%), "
            f"rx {format_bytes(rx_delta / interval if interval > 0 else 0.0)}/s"
        )
        if self.live_player is not None:
            state = "playing" if self.live_player.is_running() else "buffering"
            line += (
                f", feed {format_bytes(feed_delta / interval if interval > 0 else 0.0)}/s, "
                f"buffer {format_bytes(self.live_buffer_bytes())}, "
                f"ready {self.next_playback_chunk}/{self.total_chunks}, "
                f"state {state}"
            )
            if self.chunks_received > self.next_playback_chunk:
                line += f", wait {self.next_playback_chunk}"
        return line

    def live_buffer_bytes(self) -> int:
        player_pending = self.live_player.buffered_bytes() if self.live_player is not None else 0
        return self.playback_pending_bytes + player_pending

    def abort(self) -> None:
        if self.live_player is not None:
            self.live_player.terminate()
        self.transport_fp.close()

    def close_partial(self, reason: str) -> None:
        self._drain_live_stream()
        self.transport_fp.flush()
        self.transport_fp.close()
        if self.live_player is not None:
            self.live_player.finish()
        self._write_manifest(completed=False, reason=reason)

    def finalize(self, auto_remux: bool) -> tuple[Path, Path | None]:
        self._drain_live_stream()
        self.transport_fp.flush()
        self.transport_fp.close()

        if self.live_player is not None:
            self.live_player.finish()

        actual_sha256 = compute_sha256(self.transport_path)
        if actual_sha256 != self.transport_sha256:
            raise ValueError(
                f"SHA256 mismatch for session {self.session_id:016x}: "
                f"expected {self.transport_sha256}, got {actual_sha256}"
            )

        restored_path: Path | None = None
        restore_hint = self.metadata.get("restore_hint")
        if auto_remux and restore_hint and restore_hint.get("type") == "ffmpeg_remux":
            ffmpeg = get_ffmpeg_executable()
            target_name = safe_filename(restore_hint.get("target_name", self.original_name))
            if ffmpeg:
                candidate = self.session_dir / target_name
                command = [
                    ffmpeg,
                    "-y",
                    "-i",
                    str(self.transport_path),
                    "-c",
                    "copy",
                    str(candidate),
                ]
                result = subprocess.run(
                    command,
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                    check=False,
                )
                if result.returncode == 0 and candidate.exists() and candidate.stat().st_size > 0:
                    restored_path = candidate

        self._write_manifest(completed=True)
        return self.transport_path, restored_path

    def _write_manifest(self, completed: bool, reason: str | None = None) -> None:
        manifest = {
            **self.metadata,
            "completed": completed,
            "reason": reason,
            "chunks_received": self.chunks_received,
            "missing_chunks": self.total_chunks - self.chunks_received,
            "playback_bytes_emitted": self.playback_bytes_emitted,
        }
        self.manifest_path.write_text(
            json.dumps(manifest, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

    def _write_chunk(self, chunk_id: int, payload: bytes) -> bytes:
        payload = self._normalize_chunk(chunk_id, payload)
        self.transport_fp.seek(chunk_id * self.chunk_size)
        self.transport_fp.write(payload)
        self.received[chunk_id] = 1
        self.chunks_received += 1
        self.received_payload_bytes += len(payload)
        self._track_live_stream(chunk_id, payload)
        self.maybe_flush()
        return payload

    def _normalize_chunk(self, chunk_id: int, payload: bytes) -> bytes:
        expected = infer_chunk_length(chunk_id, self.total_chunks, self.transport_size, self.chunk_size)
        if len(payload) != expected:
            if len(payload) > expected:
                return payload[:expected]
            return payload + b"\x00" * (expected - len(payload))
        return payload

    def _track_live_stream(self, chunk_id: int, payload: bytes) -> None:
        if self.live_player is None or chunk_id < self.next_playback_chunk:
            return
        if chunk_id in self.playback_pending:
            return
        self.playback_pending[chunk_id] = payload
        self.playback_pending_bytes += len(payload)
        self._drain_live_stream()

    def _drain_live_stream(self) -> None:
        if self.live_player is None:
            return

        advanced = False
        while True:
            payload = self.playback_pending.pop(self.next_playback_chunk, None)
            if payload is None:
                break
            self.playback_pending_bytes -= len(payload)
            self.live_player.feed(payload)
            self.playback_bytes_emitted += len(payload)
            self.next_playback_chunk += 1
            advanced = True
        if advanced:
            self.last_ready_advance = time.monotonic()

    def _service_playback_waiting(self) -> list[str]:
        if self.live_player is None:
            return []

        messages: list[str] = []
        now = time.monotonic()
        buffered = self.live_player.buffered_bytes()
        stalled = now - self.last_ready_advance

        if (
            not self.live_player.is_running()
            and self.next_playback_chunk > 0
            and buffered >= self.playback_min_start_bytes
            and stalled >= self.playback_start_timeout
        ):
            if self.live_player.force_start():
                messages.append(
                    f"Playback    : session {self.session_id:016x} starting early with "
                    f"{format_bytes(buffered)} contiguous data after waiting {stalled:.1f}s"
                )
                messages.extend(self.live_player.service())

        if (
            self.chunks_received > self.next_playback_chunk
            and stalled >= 1.0
            and (
                self.last_wait_report_chunk != self.next_playback_chunk
                or now - self.last_wait_report_at >= 5.0
            )
        ):
            messages.append(
                f"Playback    : session {self.session_id:016x} waiting for chunk "
                f"{self.next_playback_chunk} before playback can continue"
            )
            self.last_wait_report_chunk = self.next_playback_chunk
            self.last_wait_report_at = now

        return messages

    def _is_ts_transport(self) -> bool:
        return self.metadata.get("media_type") == "video" and Path(self.transport_name).suffix.lower() == ".ts"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Receive USRP-forwarded UDP media, rebuild the file, and optionally play video live from contiguous data."
    )
    parser.add_argument("--bind-host", default="0.0.0.0", help="Local host/IP to bind.")
    parser.add_argument("--port", required=True, type=int, help="Local UDP port to listen on.")
    parser.add_argument("--output-dir", default="received", help="Directory for reconstructed files.")
    parser.add_argument("--idle-timeout", type=float, default=5.0, help="Seconds of silence before timing out a session.")
    parser.add_argument("--socket-buffer-kb", type=int, default=32768, help="Socket receive buffer in KiB.")
    parser.add_argument(
        "--packet-queue-size",
        type=int,
        default=8192,
        help="Number of UDP packets buffered in userspace before processing.",
    )
    parser.add_argument("--once", action="store_true", help="Exit after one successful session.")
    parser.add_argument(
        "--live-play",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Automatically start the built-in Python live player for incoming MPEG-TS video once the startup buffer is filled.",
    )
    parser.add_argument(
        "--playback-buffer-kb",
        type=int,
        default=1024,
        help="Contiguous TS data to buffer before starting live playback.",
    )
    parser.add_argument(
        "--playback-min-start-kb",
        type=int,
        default=256,
        help="If contiguous data stalls before reaching --playback-buffer-kb, allow an early start from this much buffered data.",
    )
    parser.add_argument(
        "--playback-start-timeout-ms",
        type=int,
        default=1000,
        help="How long to wait for more contiguous data before starting early with the current contiguous buffer.",
    )
    parser.add_argument(
        "--playback-rewind-kb",
        type=int,
        default=4096,
        help="Recent live data kept for automatic player restart after the playback window is closed.",
    )
    parser.add_argument(
        "--playback-no-display",
        action="store_true",
        help="Decode in real time without opening a playback window. Useful for headless testing.",
    )
    parser.add_argument(
        "--auto-remux",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="If the sender used MPEG-TS preprocessing and packaged ffmpeg is available, restore the original container.",
    )
    return parser.parse_args()


def create_session(
    output_dir: Path,
    session_id: int,
    metadata: dict,
    live_play: bool,
    playback_buffer_kb: int,
    playback_min_start_kb: int,
    playback_start_timeout_ms: int,
    playback_rewind_kb: int,
    playback_no_display: bool,
) -> SessionState:
    session_dir = output_dir / f"session_{session_id:016x}"
    session_dir.mkdir(parents=True, exist_ok=True)

    transport_name = safe_filename(metadata["transport_name"])
    original_name = safe_filename(metadata["original_name"])
    transport_path = session_dir / transport_name
    manifest_path = session_dir / "manifest.json"
    transport_fp = transport_path.open("wb+")

    live_player: LivePlayer | None = None
    playback_disabled_reason: str | None = None
    is_ts_video = metadata.get("media_type") == "video" and Path(transport_name).suffix.lower() == ".ts"
    chunk_size = int(metadata["chunk_size"])
    if live_play and is_ts_video:
        live_player = LivePlayer(
            session_id=session_id,
            transport_name=transport_name,
            startup_threshold=max(playback_buffer_kb, 0) * 1024,
            replay_buffer_limit=max(playback_rewind_kb, 0) * 1024,
            restart_delay=0.5,
            no_display=playback_no_display,
        )
    elif live_play and metadata.get("media_type") == "video":
        playback_disabled_reason = "live playback requires MPEG-TS transport; keep sender --streamable-ts enabled"

    if is_ts_video and chunk_size % TS_PACKET_SIZE != 0:
        warning = (
            f"chunk size {chunk_size} is not aligned to {TS_PACKET_SIZE}-byte TS packets; "
            "streaming still works, but sender --chunk-size 1316 is preferred for MPEG-TS."
        )
        playback_disabled_reason = f"{playback_disabled_reason}; {warning}" if playback_disabled_reason else warning

    total_chunks = int(metadata["total_chunks"])
    return SessionState(
        session_id=session_id,
        metadata=metadata,
        session_dir=session_dir,
        transport_path=transport_path,
        manifest_path=manifest_path,
        chunk_size=chunk_size,
        total_chunks=total_chunks,
        transport_size=int(metadata["transport_size"]),
        transport_sha256=metadata["transport_sha256"],
        transport_name=transport_name,
        original_name=original_name,
        transport_fp=transport_fp,
        received=bytearray(total_chunks),
        live_player=live_player,
        playback_disabled_reason=playback_disabled_reason,
        playback_min_start_bytes=max(playback_min_start_kb, 0) * 1024,
        playback_start_timeout=max(playback_start_timeout_ms, 0) / 1000.0,
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


def finalize_session(state: SessionState, auto_remux: bool) -> None:
    transport_path, restored_path = state.finalize(auto_remux=auto_remux)
    print(f"Completed   : session {state.session_id:016x}")
    print(f"Transport   : {transport_path}")
    if restored_path:
        print(f"Restored    : {restored_path}")
    print(f"Chunks      : {state.chunks_received}/{state.total_chunks}")
    print(f"Size        : {format_bytes(state.transport_size)}")
    if state.live_player is not None:
        print(
            f"Playback    : fed {format_bytes(state.playback_bytes_emitted)} contiguous data, "
            f"ready {state.next_playback_chunk}/{state.total_chunks}"
        )


def close_incomplete_session(state: SessionState, reason: str) -> None:
    state.close_partial(reason=reason)
    print(f"Incomplete  : session {state.session_id:016x}, {reason}")
    print(f"Chunks      : {state.chunks_received}/{state.total_chunks}")
    if state.live_player is not None:
        print(
            f"Playback    : fed {format_bytes(state.playback_bytes_emitted)} contiguous data, "
            f"ready {state.next_playback_chunk}/{state.total_chunks}"
        )
        if state.next_playback_chunk < state.total_chunks:
            print(f"Playback    : first missing chunk for playback was {state.next_playback_chunk}")


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
    last_report = time.monotonic()

    print(f"Listening   : {args.bind_host}:{args.port}")
    print(f"Output dir  : {output_dir}")
    print(f"Socket buf  : requested {format_bytes(args.socket_buffer_kb * 1024)}, actual {format_bytes(actual_socket_buffer)}")
    print(f"Queue size  : {args.packet_queue_size} packets")

    try:
        while True:
            try:
                packet, peer = packet_queue.get(timeout=0.1)
            except queue.Empty:
                packet = None
                peer = ("", 0)

            now = time.monotonic()
            if packet is not None:
                try:
                    kind, session_id, field_a, field_b, field_c, payload = parse_packet(packet)
                except ValueError:
                    continue

                if kind == PacketKind.START:
                    metadata = loads_json(payload)
                    if session_id not in sessions:
                        state = create_session(
                            output_dir=output_dir,
                            session_id=session_id,
                            metadata=metadata,
                            live_play=args.live_play,
                            playback_buffer_kb=args.playback_buffer_kb,
                            playback_min_start_kb=args.playback_min_start_kb,
                            playback_start_timeout_ms=args.playback_start_timeout_ms,
                            playback_rewind_kb=args.playback_rewind_kb,
                            playback_no_display=args.playback_no_display,
                        )
                        sessions[session_id] = state
                        print(
                            f"Session     : {session_id:016x} from {peer[0]}:{peer[1]}, "
                            f"{metadata['media_type']} {metadata['transport_name']}"
                        )
                        if state.live_player is not None:
                            print(
                                f"Playback    : armed for session {session_id:016x}, "
                                f"startup {format_bytes(state.live_player.startup_threshold)}, "
                                f"rewind {format_bytes(state.live_player.replay_buffer_limit)}"
                            )
                        if state.playback_disabled_reason:
                            print(f"Playback    : {state.playback_disabled_reason}")
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
                            finalize_session(state, auto_remux=args.auto_remux)
                        except Exception as exc:
                            print(f"Failed      : session {session_id:016x}, {exc}")
                            return 1
                        sessions.pop(session_id, None)
                        if args.once:
                            return 0

            expired: list[int] = []
            exit_code = 0
            for session_id, state in list(sessions.items()):
                state.maybe_flush()
                for message in state.service_live_playback():
                    print(message)

                if state.end_received and state.is_complete():
                    try:
                        finalize_session(state, auto_remux=args.auto_remux)
                    except Exception as exc:
                        print(f"Failed      : session {session_id:016x}, {exc}")
                        return 1
                    expired.append(session_id)
                    continue

                idle = now - state.last_activity
                if idle > args.idle_timeout:
                    close_incomplete_session(
                        state,
                        reason=f"timed out after {args.idle_timeout:.1f}s while waiting for more correct data",
                    )
                    expired.append(session_id)
                    exit_code = 1

            for session_id in expired:
                sessions.pop(session_id, None)
                if args.once and not sessions:
                    return exit_code

            if now - last_report >= 1.0:
                interval = now - last_report
                for state in sessions.values():
                    print(state.progress_line(interval))
                last_report = now
    finally:
        stop_event.set()
        try:
            sock.close()
        except OSError:
            pass
        reader_thread.join(timeout=1.0)
        for state in sessions.values():
            state.abort()


if __name__ == "__main__":
    raise SystemExit(main())
