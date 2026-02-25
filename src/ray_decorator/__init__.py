import functools
import hashlib
import inspect
import logging
import os
import shutil
import subprocess
import tempfile
from importlib import metadata
from typing import Any, Callable

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

# --- Logging Setup ---
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)

# --- Utilities ---


def get_nested_value(kwargs: dict, path: str) -> Any:
    """Retrieves a nested value from dot-separated path in kwargs."""
    keys = path.split(".")
    current = kwargs
    for key in keys:
        if isinstance(current, functools.partial):
            current = current.keywords
        if not isinstance(current, dict) or key not in current:
            raise KeyError(f"Path '{path}' not found (failed at '{key}')")
        current = current[key]
    return current


def set_nested_value(kwargs: dict, path: str, value: Any) -> None:
    """Sets a nested value for a dot-separated path in kwargs."""
    keys = path.split(".")
    current = kwargs
    for key in keys[:-1]:
        if isinstance(current, functools.partial):
            current = current.keywords
        if not isinstance(current, dict) or key not in current:
            raise KeyError(f"Path '{path}' not found (failed at '{key}')")
        current = current[key]

    if isinstance(current, functools.partial):
        current.keywords[keys[-1]] = value
    else:
        current[keys[-1]] = value


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


def s3_sync(src: str, dst: str):
    """Syncs from src to dst (local or S3). Skips unchanged files."""
    if src.startswith("s3://") or dst.startswith("s3://"):
        # If it's a directory
        is_dir = os.path.isdir(src) if not src.startswith("s3://") else True
        if not src.startswith("s3://") and not os.path.exists(src):
            return

        if not is_aws_available():
            raise ValueError(
                "The 'aws' CLI is required for S3 synchronization. "
                "Please install it and ensure it is in your PATH."
            )
        cmd = ["aws", "s3", "sync" if is_dir else "cp", src, dst]
        if not is_dir and not dst.startswith("s3://"):
            os.makedirs(os.path.dirname(os.path.abspath(dst)), exist_ok=True)

        subprocess.run(cmd, check=True, capture_output=True)
    else:
        if not os.path.exists(src):
            return
        if os.path.isdir(src):
            if os.path.exists(dst):
                shutil.rmtree(dst)
            shutil.copytree(src, dst)
        else:
            os.makedirs(os.path.dirname(os.path.abspath(dst)), exist_ok=True)
            shutil.copy2(src, dst)


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
        s3_sync(hash_tmp, hash_s3)
    finally:
        if os.path.exists(hash_tmp):
            os.remove(hash_tmp)


# --- Worker Logic ---


def worker_wrapper(
    func: Callable, args: tuple, kwargs: dict, deps: list[str], outs: list[str]
) -> Any:
    """
    Runs on Ray worker.
    Maps S3 paths back to local worker paths, verifies hashes, runs function, and uploads results.
    Uses the task's current working directory.
    """
    worker_run_dir = os.getcwd()
    s3_out_uris = {}

    # 1. Process Inputs (S3 -> Worker Local)
    for dep_key in deps:
        s3_uri = get_nested_value(kwargs, dep_key)
        basename = os.path.basename(s3_uri.rstrip("/"))
        local_worker_path = os.path.join(worker_run_dir, "deps", basename)

        logger.info(f"[Worker] Mapping input: {s3_uri} -> {local_worker_path}")

        remote_hash = get_s3_hash(s3_uri)
        local_hash = calculate_path_hash(local_worker_path)

        if remote_hash and remote_hash == local_hash:
            logger.info(
                f"[Worker] Input '{dep_key}' matches remote hash. Skipping download."
            )
        else:
            logger.info(f"[Worker] Syncing input '{dep_key}' from S3...")
            s3_sync(s3_uri, local_worker_path)

        set_nested_value(kwargs, dep_key, local_worker_path)

    # 2. Map Outputs (S3 -> Worker Local)
    for out_key in outs:
        s3_uri = get_nested_value(kwargs, out_key)
        s3_out_uris[out_key] = s3_uri

        basename = os.path.basename(s3_uri.rstrip("/"))
        local_worker_path = os.path.join(worker_run_dir, "outs", basename)

        logger.info(f"[Worker] Mapping output: {s3_uri} -> {local_worker_path}")
        set_nested_value(kwargs, out_key, local_worker_path)

    # 3. Run the actual function
    logger.info(f"[Worker] Starting execution of '{func.__name__}'...")
    logger.info(f"[Worker] Function arguments: {kwargs}")
    # result = func(**kwargs)
    logger.info(f"[Worker] Execution of '{func.__name__}' completed.")

    # 4. Process Outputs (Worker Local -> S3)
    for out_key, s3_uri in s3_out_uris.items():
        local_path = get_nested_value(kwargs, out_key)
        if os.path.exists(local_path):
            out_hash = calculate_path_hash(local_path)
            remote_hash = get_s3_hash(s3_uri)

            if out_hash == remote_hash:
                logger.info(
                    f"[Worker] Output '{out_key}' matches remote S3 hash. Skipping upload."
                )
            else:
                logger.info(f"[Worker] Uploading output '{out_key}' to S3...")
                s3_sync(local_path, s3_uri)
                upload_hash(s3_uri, out_hash)
        else:
            logger.warning(
                f"[Worker] Output '{out_key}' not found at {local_path} after execution."
            )

    return result


# --- Decorator ---


def ray_decorator(
    deps: list[str],
    outs: list[str],
    ray_address: str | None = None,
    s3_base_path: str | None = None,
    working_dir: str | Any | None = None,
) -> Callable:
    """
    Decorator to offload execution to Ray with symmetric S3 sync and MD5 deduplication.

    Args:
        deps: List of dot-separated paths in kwargs to be treated as dependencies (synced TO s3).
        outs: List of dot-separated paths in kwargs to be treated as outputs (synced FROM s3).
        ray_address: Address of the Ray cluster (e.g. 'ray://1.2.3.4:10001').
                     Defaults to RAY_ADDRESS env var.
        s3_base_path: S3 path for intermediate storage (e.g. 's3://my-bucket/jobs').
                      Defaults to RAY_S3_BASE_PATH env var.
        working_dir: Local directory to sync to workers. Use "." for current project.
    """

    def decorator(func: Callable) -> Callable:
        if not is_ray_available():
            raise ValueError(
                "The 'ray' package is required to use @ray_decorator. "
                "Please install it with 'pip install ray'."
            )

        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            import ray
            from ray.runtime_env import RuntimeEnv

            # Resolve all arguments
            sig = inspect.signature(func)
            bound = sig.bind(*args, **kwargs)
            bound.apply_defaults()
            bound_kwargs = bound.arguments

            # --- 0. Resolve Dynamic Configs ---
            final_ray_address = ray_address or os.environ.get("RAY_ADDRESS")
            final_s3_base_path = s3_base_path or os.environ.get("RAY_S3_BASE_PATH")

            if not final_ray_address:
                raise ValueError(
                    "Ray address must be provided via 'ray_address' argument "
                    "or 'RAY_ADDRESS' environment variable."
                )
            if not final_s3_base_path:
                raise ValueError(
                    "S3 base path must be provided via 's3_base_path' argument "
                    "or 'RAY_S3_BASE_PATH' environment variable."
                )

            # --- 1. Process Inputs (Driver Local -> S3) ---
            # Stable run_id based on dependency contents
            combined_hash = hashlib.md5()
            for dep_key in deps:
                val = get_nested_value(bound_kwargs, dep_key)
                if val:
                    combined_hash.update(calculate_path_hash(str(val)).encode())

            run_id = combined_hash.hexdigest()
            s3_base = f"{final_s3_base_path.rstrip('/')}/{run_id}"
            logger.info(f"[Driver] Using stable Run ID: {run_id}")

            for dep_key in deps:
                local_path = str(get_nested_value(bound_kwargs, dep_key))
                s3_uri = f"{s3_base}/deps/{os.path.basename(local_path.rstrip('/'))}"

                logger.info(f"[Driver] Mapping input: {local_path} -> {s3_uri}")

                local_hash = calculate_path_hash(local_path)
                remote_hash = get_s3_hash(s3_uri)

                if local_hash and local_hash == remote_hash:
                    logger.info(
                        f"[Driver] Input '{dep_key}' matches remote S3 hash. Skipping upload."
                    )
                else:
                    logger.info(f"[Driver] Uploading input '{dep_key}' to S3...")
                    s3_sync(local_path, s3_uri)
                    upload_hash(s3_uri, local_hash)

                set_nested_value(bound_kwargs, dep_key, s3_uri)

            # --- 2. Map Outputs (Driver Local -> S3) ---
            original_local_outs = {}
            for out_key in outs:
                local_path = str(get_nested_value(bound_kwargs, out_key))
                original_local_outs[out_key] = local_path
                s3_uri = f"{s3_base}/outs/{os.path.basename(local_path.rstrip('/'))}"

                logger.info(f"[Driver] Mapping output: {local_path} -> {s3_uri}")
                set_nested_value(bound_kwargs, out_key, s3_uri)

            # --- 3. Remote Execution ---
            if not ray.is_initialized():
                pkgs = [
                    f"{d.metadata['Name']}=={d.version}"
                    for d in metadata.distributions()
                    if d.metadata["Name"] != "ray-decorator"
                ]
                logger.info(
                    f"[Driver] Initializing Ray at {final_ray_address} with {len(pkgs)} packages..."
                )
                ray.init(
                    address=final_ray_address,
                    runtime_env=RuntimeEnv(
                        working_dir=working_dir,
                        uv={"packages": pkgs},
                        env_vars={
                            k: os.environ.get(k)
                            for k in ["AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY"]
                            if k in os.environ
                        },
                    ),
                )

            if "RAY_RUNTIME_ENV_HOOK" not in os.environ:
                os.environ["RAY_RUNTIME_ENV_HOOK"] = (
                    "ray._private.runtime_env.uv_runtime_env_hook.hook"
                )

            logger.info(f"[Driver] Submitting '{func.__name__}' to Ray cluster...")
            logger.info(f"[Driver] Bound args: {bound.args}")
            logger.info(f"[Driver] Bound kwargs: {bound_kwargs}")
            logger.info(f"[Driver] Deps: {deps}")
            logger.info(f"[Driver] Outs: {outs}")
            remote_wrapper = ray.remote(worker_wrapper)
            result = ray.get(
                remote_wrapper.remote(func, bound.args, bound_kwargs, deps, outs)
            )
            logger.info(f"[Driver] Remote execution completed.")

            # --- 4. Retrieval & Deduplication (Driver) ---
            for out_key, local_path in original_local_outs.items():
                s3_uri = get_nested_value(bound_kwargs, out_key)

                remote_hash = get_s3_hash(s3_uri)
                local_hash = calculate_path_hash(local_path)

                if remote_hash and remote_hash == local_hash:
                    logger.info(
                        f"[Driver] Output '{out_key}' matches current local content. Skipping download."
                    )
                else:
                    logger.info(f"[Driver] Downloading output '{out_key}' from S3...")
                    s3_sync(s3_uri, local_path)

            return result

        return wrapper

    return decorator
