from __future__ import annotations

import hashlib
import json
import os
import tempfile
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath


EMPTY_SHA256 = hashlib.sha256(b"").hexdigest()


@dataclass(slots=True, frozen=True)
class EntrySnapshot:
    root_name: str
    relative_path: str
    absolute_path: str
    entry_type: str
    size: int
    modified_ns: int

    @property
    def signature(self) -> tuple[str, int, int]:
        return (self.entry_type, self.size, self.modified_ns)


@dataclass(slots=True)
class StagedFile:
    source_path: Path
    staged_path: Path
    size: int
    sha256: str
    modified_ns: int

    def cleanup(self) -> None:
        try:
            self.staged_path.unlink()
        except FileNotFoundError:
            pass


def ceil_div(value: int, divisor: int) -> int:
    if divisor <= 0:
        raise ValueError("divisor must be > 0")
    return (value + divisor - 1) // divisor


def compute_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def dumps_json(payload: dict) -> bytes:
    return json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")


def loads_json(payload: bytes) -> dict:
    return json.loads(payload.decode("utf-8"))


def format_bytes(size: float) -> str:
    units = ["B", "KB", "MB", "GB", "TB"]
    value = float(size)
    for unit in units:
        if value < 1024.0 or unit == units[-1]:
            return f"{value:.2f} {unit}"
        value /= 1024.0
    return f"{value:.2f} TB"


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def normalize_relative_path(value: str) -> str:
    candidate = value.replace("\\", "/")
    pure = PurePosixPath(candidate)
    if pure.is_absolute():
        raise ValueError(f"absolute path is not allowed: {value!r}")

    parts: list[str] = []
    for part in pure.parts:
        if part in ("", "."):
            continue
        if part == "..":
            raise ValueError(f"parent traversal is not allowed: {value!r}")
        parts.append(part)

    if not parts:
        raise ValueError("relative path must not be empty")
    return "/".join(parts)


def resolve_output_path(output_dir: Path, relative_path: str) -> Path:
    normalized = normalize_relative_path(relative_path)
    candidate = output_dir.joinpath(*PurePosixPath(normalized).parts)
    output_dir_resolved = output_dir.resolve()
    candidate_resolved = candidate.resolve(strict=False)
    if os.path.commonpath([str(output_dir_resolved), str(candidate_resolved)]) != str(output_dir_resolved):
        raise ValueError(f"path escapes output directory: {relative_path!r}")
    return candidate


def iter_directory_entries(root: Path) -> list[EntrySnapshot]:
    root = root.resolve()
    root_name = root.name
    entries: list[EntrySnapshot] = []
    for candidate in sorted(root.rglob("*"), key=lambda path: path.relative_to(root).as_posix()):
        if candidate.is_symlink():
            continue
        if not (candidate.is_file() or candidate.is_dir()):
            continue
        relative_path = candidate.relative_to(root).as_posix()
        stat = candidate.stat()
        entry_type = "directory" if candidate.is_dir() else "file"
        size = 0 if entry_type == "directory" else stat.st_size
        entries.append(
            EntrySnapshot(
                root_name=root_name,
                relative_path=relative_path,
                absolute_path=str(candidate),
                entry_type=entry_type,
                size=size,
                modified_ns=stat.st_mtime_ns,
            )
        )
    return entries


def stage_file_snapshot(snapshot: EntrySnapshot) -> StagedFile | None:
    if snapshot.entry_type != "file":
        raise ValueError("stage_file_snapshot only supports file entries")

    source_path = Path(snapshot.absolute_path)
    try:
        stat_before = source_path.stat()
    except FileNotFoundError:
        return None

    if stat_before.st_size != snapshot.size or stat_before.st_mtime_ns != snapshot.modified_ns:
        return None

    digest = hashlib.sha256()
    fd, temp_name = tempfile.mkstemp(prefix="usrp_udp_send_", suffix=".part")
    staged_path = Path(temp_name)
    total_bytes = 0

    try:
        with os.fdopen(fd, "wb") as destination, source_path.open("rb") as source:
            while True:
                chunk = source.read(1024 * 1024)
                if not chunk:
                    break
                destination.write(chunk)
                digest.update(chunk)
                total_bytes += len(chunk)
    except Exception:
        try:
            staged_path.unlink()
        except FileNotFoundError:
            pass
        raise

    try:
        stat_after = source_path.stat()
    except FileNotFoundError:
        staged_path.unlink(missing_ok=True)
        return None

    if (
        stat_after.st_size != snapshot.size
        or stat_after.st_mtime_ns != snapshot.modified_ns
        or total_bytes != snapshot.size
    ):
        staged_path.unlink(missing_ok=True)
        return None

    return StagedFile(
        source_path=source_path,
        staged_path=staged_path,
        size=total_bytes,
        sha256=digest.hexdigest(),
        modified_ns=snapshot.modified_ns,
    )


def build_entry_metadata(
    snapshot: EntrySnapshot,
    chunk_size: int,
    content_sha256: str,
    sequence_number: int,
) -> dict:
    total_chunks = ceil_div(snapshot.size, chunk_size) if chunk_size > 0 else 0
    return {
        "protocol": "usrp_udp_directory_v2",
        "created_at": utc_now_iso(),
        "sequence_number": sequence_number,
        "root_name": snapshot.root_name,
        "relative_path": snapshot.relative_path,
        "entry_type": snapshot.entry_type,
        "chunk_size": chunk_size,
        "content_size": snapshot.size,
        "total_chunks": total_chunks,
        "content_sha256": content_sha256,
        "content_sha256_algo": "sha256",
        "modified_ns": snapshot.modified_ns,
    }


def infer_chunk_length(chunk_id: int, total_chunks: int, total_size: int, chunk_size: int) -> int:
    if total_chunks == 0:
        raise ValueError("zero-chunk entries do not have chunk lengths")
    if chunk_id < 0 or chunk_id >= total_chunks:
        raise ValueError("chunk_id out of range")
    if chunk_id == total_chunks - 1:
        remainder = total_size % chunk_size
        return remainder or chunk_size
    return chunk_size
