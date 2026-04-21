import functools
import hashlib
import os
import shutil
from dataclasses import dataclass
from typing import Any, Literal


@dataclass
class PathData:
    path: str
    location: Literal["local", "s3"]
    is_dir: bool | None = None
    hash: str | None = None

    def __repr__(self) -> str:
        try:
            # Try printing the emoji, fallback if UnicodeEncodeError
            emoji = "🖥️" if self.location == "local" else "☁️"
            # Attempt to encode to the console's encoding
            emoji.encode(encoding=os.device_encoding(1) or "utf-8")
        except Exception:
            emoji = "LOCAL" if self.location == "local" else "REMOTE"
        return f"{emoji} {self.path}"


@dataclass
class PathMatch:
    local: PathData
    remote: PathData

    def __repr__(self) -> str:
        return f"{self.local} -> {self.remote}"


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
