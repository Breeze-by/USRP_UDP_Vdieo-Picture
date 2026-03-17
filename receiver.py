from __future__ import annotations

import argparse
import json
import queue
import shutil
import socket
import subprocess
import threading
import time
from collections import deque
from dataclasses import dataclass, field
from pathlib import Path
from typing import BinaryIO

from usrp_udp.common import (
    compute_sha256,
    format_bytes,
    infer_chunk_length,
    loads_json,
    safe_filename,
)
from usrp_udp.protocol import PacketKind, parse_packet


TS_PACKET_SIZE = 188


@dataclass(slots=True)
class LivePlayer:
    session_id: int
    transport_name: str
    executable: str
    startup_threshold: int
    replay_buffer_limit: int
    restart_delay: float
    no_display: bool = False
    process: subprocess.Popen[bytes] | None = None
    writer_thread: threading.Thread | None = None
    current_queue: queue.SimpleQueue[bytes | None] | None = None
    pending_chunks: deque[bytes] = field(default_factory=deque)
    replay_history: deque[bytes] = field(default_factory=deque)
    events: queue.SimpleQueue[str] = field(default_factory=queue.SimpleQueue)
    lock: threading.Lock = field(default_factory=threading.Lock)
    pending_bytes: int = 0
    queued_bytes: int = 0
    replay_history_bytes: int = 0
    restart_pending_reason: str | None = None
    restart_count: int = 0
    stop_requested: bool = False
    last_start_attempt: float = 0.0

    def feed(self, payload: bytes) -> None:
        if not payload:
            return

        now = time.monotonic()
        with self.lock:
            self._service_locked(now)
            self._append_replay_locked(payload)
            if self.process is None:
                self.pending_chunks.append(payload)
                self.pending_bytes += len(payload)
                self._maybe_start_locked(now)
                return
            queue_obj = self.current_queue
            if queue_obj is not None:
                self.queued_bytes += len(payload)

        if queue_obj is not None:
            queue_obj.put(payload)

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
            return self.pending_bytes + self.queued_bytes

    def finish(self) -> None:
        with self.lock:
            self.stop_requested = True
            now = time.monotonic()
            self._service_locked(now)
            if self.process is None and self.pending_bytes > 0:
                self._maybe_start_locked(now, force=True)
            queue_obj = self.current_queue
            writer_thread = self.writer_thread
            process = self.process
            self.queued_bytes = 0

        if queue_obj is not None:
            queue_obj.put(None)
        if writer_thread is not None:
            writer_thread.join(timeout=5.0)
        if process is not None:
            try:
                process.wait(timeout=5.0)
            except subprocess.TimeoutExpired:
                process.terminate()
                try:
                    process.wait(timeout=2.0)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=2.0)

    def terminate(self) -> None:
        with self.lock:
            self.stop_requested = True
            queue_obj = self.current_queue
            writer_thread = self.writer_thread
            process = self.process
            self.current_queue = None
            self.writer_thread = None
            self.process = None
            self.queued_bytes = 0

        if queue_obj is not None:
            queue_obj.put(None)
        if process is not None and process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=2.0)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=2.0)
        if writer_thread is not None:
            writer_thread.join(timeout=1.0)

    def _append_replay_locked(self, payload: bytes) -> None:
        self.replay_history.append(payload)
        self.replay_history_bytes += len(payload)
        while self.replay_history and self.replay_history_bytes > self.replay_buffer_limit:
            dropped = self.replay_history.popleft()
            self.replay_history_bytes -= len(dropped)

    def _service_locked(self, now: float) -> None:
        if self.process is None or self.process.poll() is None:
            return

        reason = f"ffplay exited with code {self.process.returncode}"
        self.current_queue = None
        self.writer_thread = None
        self.process = None
        if self.stop_requested:
            return

        self.pending_chunks = deque(self.replay_history)
        self.pending_bytes = self.replay_history_bytes
        self.queued_bytes = 0
        self.restart_pending_reason = reason
        self.events.put(
            f"Playback    : session {self.session_id:016x} player stopped ({reason}), "
            f"rebuffering {format_bytes(self.pending_bytes)}"
        )
        self._maybe_start_locked(now)

    def _maybe_start_locked(self, now: float, force: bool = False) -> None:
        if self.stop_requested or self.process is not None:
            return
        if not force and self.pending_bytes < self.startup_threshold:
            return
        if now - self.last_start_attempt < self.restart_delay:
            return

        self.last_start_attempt = now

        command = [
            self.executable,
            "-hide_banner",
            "-loglevel",
            "warning",
            "-window_title",
            f"USRP UDP Live {self.transport_name}",
        ]
        if self.no_display:
            command.append("-nodisp")
        command.extend(
            [
                "-autoexit",
                "-probesize",
                "32",
                "-analyzeduration",
                "0",
                "-f",
                "mpegts",
                "-i",
                "pipe:0",
            ]
        )

        try:
            process = subprocess.Popen(
                command,
                stdin=subprocess.PIPE,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                bufsize=0,
            )
        except OSError as exc:
            self.events.put(f"Playback    : session {self.session_id:016x} failed to start ffplay ({exc})")
            return

        queue_obj: queue.SimpleQueue[bytes | None] = queue.SimpleQueue()
        writer_thread = threading.Thread(
            target=self._writer_loop,
            args=(process, queue_obj),
            name=f"live-player-{self.session_id:016x}",
            daemon=True,
        )
        writer_thread.start()

        self.process = process
        self.current_queue = queue_obj
        self.writer_thread = writer_thread

        if self.restart_pending_reason is None:
            self.events.put(
                f"Playback    : session {self.session_id:016x} started, "
                f"startup buffer {format_bytes(max(self.startup_threshold, self.pending_bytes))}"
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
            self.queued_bytes += len(payload)
            queue_obj.put(payload)
        self.pending_bytes = 0

    def _writer_loop(self, process: subprocess.Popen[bytes], write_queue: queue.SimpleQueue[bytes | None]) -> None:
        if process.stdin is None:
            return

        try:
            while True:
                payload = write_queue.get()
                if payload is None:
                    break
                if process.poll() is not None:
                    break
                process.stdin.write(payload)
                process.stdin.flush()
                with self.lock:
                    self.queued_bytes = max(0, self.queued_bytes - len(payload))
        except (BrokenPipeError, OSError):
            pass
        finally:
            try:
                process.stdin.close()
            except OSError:
                pass


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
    last_activity: float = field(default_factory=time.monotonic)
    last_flush: float = field(default_factory=time.monotonic)
    last_report_rx_bytes: int = 0
    last_report_play_bytes: int = 0

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
        return messages

    def progress_line(self, interval: float) -> str:
        rx_delta = self.received_payload_bytes - self.last_report_rx_bytes
        play_delta = self.playback_bytes_emitted - self.last_report_play_bytes
        self.last_report_rx_bytes = self.received_payload_bytes
        self.last_report_play_bytes = self.playback_bytes_emitted

        progress = self.chunks_received / self.total_chunks * 100.0 if self.total_chunks else 100.0
        line = (
            f"Progress    : session {self.session_id:016x} "
            f"{self.chunks_received}/{self.total_chunks} chunks ({progress:.1f}%), "
            f"rx {format_bytes(rx_delta / interval if interval > 0 else 0.0)}/s"
        )
        if self.live_player is not None:
            line += (
                f", play {format_bytes(play_delta / interval if interval > 0 else 0.0)}/s, "
                f"buffer {format_bytes(self.live_buffer_bytes())}, "
                f"ready {self.next_playback_chunk}/{self.total_chunks}"
            )
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
            ffmpeg = shutil.which("ffmpeg")
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

        while True:
            payload = self.playback_pending.pop(self.next_playback_chunk, None)
            if payload is None:
                break
            self.playback_pending_bytes -= len(payload)
            self.live_player.feed(payload)
            self.playback_bytes_emitted += len(payload)
            self.next_playback_chunk += 1

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
    parser.add_argument("--socket-buffer-kb", type=int, default=4096, help="Socket receive buffer in KiB.")
    parser.add_argument("--once", action="store_true", help="Exit after one successful session.")
    parser.add_argument(
        "--live-play",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Automatically start ffplay for incoming MPEG-TS video once the startup buffer is filled.",
    )
    parser.add_argument(
        "--playback-buffer-kb",
        type=int,
        default=2048,
        help="Contiguous TS data to buffer before starting live playback.",
    )
    parser.add_argument(
        "--playback-rewind-kb",
        type=int,
        default=4096,
        help="Recent live data kept for automatic ffplay restart after the player is closed.",
    )
    parser.add_argument(
        "--playback-no-display",
        action="store_true",
        help="Start ffplay in no-display mode. Useful for headless testing.",
    )
    parser.add_argument(
        "--auto-remux",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="If the sender used MPEG-TS preprocessing and ffmpeg is available, restore the original container.",
    )
    return parser.parse_args()


def create_session(
    output_dir: Path,
    session_id: int,
    metadata: dict,
    live_play: bool,
    playback_buffer_kb: int,
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
        executable = shutil.which("ffplay")
        if executable is None:
            playback_disabled_reason = "ffplay not found"
        else:
            live_player = LivePlayer(
                session_id=session_id,
                transport_name=transport_name,
                executable=executable,
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
    )


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
            f"Playback    : fed {format_bytes(state.playback_bytes_emitted)} to player, "
            f"ready {state.next_playback_chunk}/{state.total_chunks}"
        )


def close_incomplete_session(state: SessionState, reason: str) -> None:
    state.close_partial(reason=reason)
    print(f"Incomplete  : session {state.session_id:016x}, {reason}")
    print(f"Chunks      : {state.chunks_received}/{state.total_chunks}")
    if state.live_player is not None:
        print(
            f"Playback    : fed {format_bytes(state.playback_bytes_emitted)} to player, "
            f"ready {state.next_playback_chunk}/{state.total_chunks}"
        )


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

    sessions: dict[int, SessionState] = {}
    last_report = time.monotonic()

    print(f"Listening   : {args.bind_host}:{args.port}")
    print(f"Output dir  : {output_dir}")

    try:
        while True:
            try:
                packet, peer = sock.recvfrom(65535)
            except socket.timeout:
                packet = None

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
        for state in sessions.values():
            state.abort()
        sock.close()


if __name__ == "__main__":
    raise SystemExit(main())
