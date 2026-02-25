import functools
import hashlib
import logging
import os
import shutil
import subprocess
import tempfile
from dataclasses import dataclass
from typing import Any, Literal

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)


@dataclass
class PathData:
    path: str
    location: Literal["local", "s3"]
    is_dir: bool | None = None
    hash: str | None = None


@dataclass
class PathMatch:
    local: PathData
    remote: PathData


def is_ray_available() -> bool:
    """Checks if the 'ray' package is installed."""
    try:
        import ray  # noqa: F401

        return True
    except ImportError:
        return False


def is_aws_available() -> bool:
    """Checks if the 'aws' CLI is available in the system PATH."""
    return shutil.which("aws") is not None


def get_nested_value(container: Any, path: str) -> Any:
    """Retrieves a nested value from dot-separated path in a dict or DictConfig."""
    keys = path.split(".")
    current = container
    for key in keys:
        if isinstance(current, functools.partial):
            current = current.keywords
        # Support for DictConfig or dict
        if hasattr(current, "__getitem__"):
            current = current[key]
        else:
            raise KeyError(f"Path '{path}' not found (failed at '{key}')")
    return current


def set_nested_value(container: Any, path: str, value: Any) -> None:
    """Sets a nested value for a dot-separated path in a dict or DictConfig."""
    keys = path.split(".")
    current = container
    for key in keys[:-1]:
        if isinstance(current, functools.partial):
            current = current.keywords
        # Support for DictConfig or dict
        if hasattr(current, "__getitem__"):
            current = current[key]
        else:
            raise KeyError(f"Path '{path}' not found (failed at '{key}')")

    if isinstance(current, functools.partial):
        current.keywords[keys[-1]] = value
    elif hasattr(current, "__setitem__"):
        current[keys[-1]] = value
    else:
        setattr(current, keys[-1], value)


def calculate_path_hash(path: str) -> str:
    """Calculates recursive MD5 for file or directory."""
    if not os.path.exists(path):
        return ""
    hash_md5 = hashlib.md5()
    if os.path.isfile(path):
        with open(path, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_md5.update(chunk)
    else:
        for root, dirs, files in os.walk(path):
            for names in sorted(files):
                filepath = os.path.join(root, names)
                hash_md5.update(os.path.relpath(filepath, path).encode())
                with open(filepath, "rb") as f:
                    for chunk in iter(lambda: f.read(4096), b""):
                        hash_md5.update(chunk)
    return hash_md5.hexdigest()


def get_s3_base_path(s3_base_path: str, deps: list[str], container: Any) -> str:
    combined_hash = hashlib.md5()
    for dep_key in deps:
        try:
            val = get_nested_value(container, dep_key)
            if val:
                combined_hash.update(calculate_path_hash(str(val)).encode())
        except (KeyError, AttributeError):
            continue

    run_id = combined_hash.hexdigest()
    s3_base = f"{s3_base_path.rstrip('/')}/{run_id}"
    logger.info(f"[Driver] Using stable Run ID: {run_id}")
    return s3_base


def s3_sync(src: PathData, dst: PathData):
    """Syncs from src to dst (local or S3). Skips unchanged files."""
    if src.location == "s3" or dst.location == "s3":
        # Determine if it's a directory
        # For local paths, use os.path.isdir
        # For S3 paths, we rely on the given is_dir flag

        if not src.path.startswith("s3://") and not os.path.exists(src.path):
            return

        if not is_aws_available():
            raise ValueError(
                "The 'aws' CLI is required for S3 synchronization. "
                "Please install it and ensure it is in your PATH."
            )
        source_path = src.path
        destination_path = dst.path

        cmd = [
            "aws",
            "s3",
            "cp" if not src.is_dir else "sync",
            source_path,
            destination_path,
        ]
        logger.info(f"[S3 Sync] Syncing '{source_path}' to '{destination_path}'...")
        subprocess.run(cmd, check=True, capture_output=True)
    else:
        if not os.path.exists(src.path):
            return
        if os.path.isdir(src.path):
            if os.path.exists(dst.path):
                shutil.rmtree(dst.path)
            shutil.copytree(src.path, dst.path)
        if not src.is_dir and not dst.path.startswith("s3://"):
            dest_dir = os.path.dirname(os.path.abspath(dst.path))
            if dest_dir:
                os.makedirs(dest_dir, exist_ok=True)


def get_s3_hash(s3_uri: str) -> str:
    """Fetches the MD5 hash from a .md5 sidecar file on S3."""
    hash_s3 = s3_uri.rstrip("/") + ".md5"
    with tempfile.NamedTemporaryFile(mode="r", delete=False) as f:
        hash_tmp = f.name
    try:
        # Check if exists first
        res = subprocess.run(["aws", "s3", "ls", hash_s3], capture_output=True)
        if res.returncode != 0:
            return ""
        s3_sync(hash_s3, hash_tmp)
        with open(hash_tmp, "r") as f:
            return f.read().strip()
    except Exception:
        return ""
    finally:
        if os.path.exists(hash_tmp):
            os.remove(hash_tmp)


def upload_hash(s3_uri: str, hash_val: str):
    """Uploads an MD5 hash as a .md5 sidecar file to S3."""
    hash_s3 = s3_uri.rstrip("/") + ".md5"
    with tempfile.NamedTemporaryFile(mode="w", delete=False) as f:
        f.write(hash_val)
        hash_tmp = f.name
    try:
        cmd = ["aws", "s3", "cp", hash_tmp, hash_s3]
        subprocess.run(cmd, check=True, capture_output=True)
    finally:
        if os.path.exists(hash_tmp):
            os.remove(hash_tmp)
