from __future__ import annotations

import hashlib
import json
import mimetypes
import re
import shutil
import subprocess
import tempfile
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path


VIDEO_EXTENSIONS = {
    ".mp4",
    ".mov",
    ".avi",
    ".mkv",
    ".wmv",
    ".flv",
    ".m4v",
    ".ts",
    ".mts",
    ".m2ts",
    ".webm",
}
IMAGE_EXTENSIONS = {
    ".jpg",
    ".jpeg",
    ".png",
    ".bmp",
    ".gif",
    ".tif",
    ".tiff",
    ".webp",
}
INVALID_FILENAME_CHARS = r'[<>:"/\\|?*\x00-\x1F]'


@dataclass(slots=True)
class PreparedMedia:
    input_path: str
    original_name: str
    transport_path: str
    transport_name: str
    media_type: str
    streamable: bool
    preprocess: dict | None
    restore_hint: dict | None


def detect_media_type(path: Path) -> str:
    suffix = path.suffix.lower()
    if suffix in VIDEO_EXTENSIONS:
        return "video"
    if suffix in IMAGE_EXTENSIONS:
        return "image"

    mime, _ = mimetypes.guess_type(path.name)
    if mime:
        if mime.startswith("video/"):
            return "video"
        if mime.startswith("image/"):
            return "image"
    return "binary"


def safe_filename(name: str) -> str:
    cleaned = re.sub(INVALID_FILENAME_CHARS, "_", name).rstrip(". ")
    if not cleaned:
        return "output.bin"
    return cleaned


def compute_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def ceil_div(value: int, divisor: int) -> int:
    return (value + divisor - 1) // divisor


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


def dumps_json(payload: dict) -> bytes:
    return json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")


def loads_json(payload: bytes) -> dict:
    return json.loads(payload.decode("utf-8"))


def prepare_media(
    input_path: Path,
    prefer_streamable_video: bool,
) -> tuple[PreparedMedia, tempfile.TemporaryDirectory[str] | None]:
    media_type = detect_media_type(input_path)
    temp_dir: tempfile.TemporaryDirectory[str] | None = None
    transport_path = input_path
    transport_name = input_path.name
    streamable = False
    preprocess = None
    restore_hint = None

    if media_type == "video" and prefer_streamable_video and input_path.suffix.lower() != ".ts":
        ffmpeg = shutil.which("ffmpeg")
        if ffmpeg:
            temp_dir = tempfile.TemporaryDirectory(prefix="usrp_udp_")
            candidate = Path(temp_dir.name) / f"{input_path.stem}_stream.ts"
            copy_cmd = [
                ffmpeg,
                "-y",
                "-i",
                str(input_path),
                "-map",
                "0",
                "-c",
                "copy",
                "-muxdelay",
                "0",
                "-muxpreload",
                "0",
                "-f",
                "mpegts",
                str(candidate),
            ]
            transcode_cmd = [
                ffmpeg,
                "-y",
                "-i",
                str(input_path),
                "-c:v",
                "libx264",
                "-preset",
                "veryfast",
                "-tune",
                "zerolatency",
                "-pix_fmt",
                "yuv420p",
                "-c:a",
                "aac",
                "-b:a",
                "128k",
                "-muxdelay",
                "0",
                "-muxpreload",
                "0",
                "-f",
                "mpegts",
                str(candidate),
            ]

            succeeded = _run_ffmpeg(copy_cmd) or _run_ffmpeg(transcode_cmd)
            if succeeded and candidate.exists() and candidate.stat().st_size > 0:
                transport_path = candidate
                transport_name = f"{input_path.stem}.ts"
                streamable = True
                preprocess = {"type": "ffmpeg_to_mpegts"}
                restore_hint = {
                    "type": "ffmpeg_remux",
                    "target_name": input_path.name,
                }
            else:
                temp_dir.cleanup()
                temp_dir = None

    prepared = PreparedMedia(
        input_path=str(input_path),
        original_name=input_path.name,
        transport_path=str(transport_path),
        transport_name=transport_name,
        media_type=media_type,
        streamable=streamable,
        preprocess=preprocess,
        restore_hint=restore_hint,
    )
    return prepared, temp_dir


def _run_ffmpeg(command: list[str]) -> bool:
    result = subprocess.run(
        command,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        check=False,
    )
    return result.returncode == 0


def build_metadata(
    prepared: PreparedMedia,
    chunk_size: int,
    transport_size: int,
    transport_sha256: str,
) -> dict:
    total_chunks = ceil_div(transport_size, chunk_size)
    return {
        "protocol": "usrp_udp_media_v1",
        "created_at": utc_now_iso(),
        **asdict(prepared),
        "chunk_size": chunk_size,
        "transport_size": transport_size,
        "total_chunks": total_chunks,
        "transport_sha256": transport_sha256,
        "transport_sha256_algo": "sha256",
    }


def infer_chunk_length(chunk_id: int, total_chunks: int, total_size: int, chunk_size: int) -> int:
    if chunk_id < 0 or chunk_id >= total_chunks:
        raise ValueError("chunk_id out of range")
    if chunk_id == total_chunks - 1:
        remainder = total_size % chunk_size
        return remainder or chunk_size
    return chunk_size
