import hashlib
import os
from importlib import metadata
from typing import Any

from .utils import (
    calculate_path_hash,
    get_nested_value,
    get_s3_hash,
    logger,
    s3_sync,
    set_nested_value,
    upload_hash,
)


def _setup_ray_cluster(ray_address: str, ray_init_kwargs: dict | None = None):
    import ray
    from ray.runtime_env import RuntimeEnv

    if not ray.is_initialized():
        pkgs = [
            f"{d.metadata['Name']}=={d.version}"
            for d in metadata.distributions()
            # if d.metadata["Name"] != "ray-decorator"
        ]
        logger.info(
            f"[Driver] Initializing Ray at {ray_address} with {len(pkgs)} packages..."
        )
        ray.shutdown()

        default_env = {
            k: os.environ.get(k)
            for k in ["AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY"]
            if k in os.environ
        }

        # Handle ray_init_kwargs
        init_kwargs = (ray_init_kwargs or {}).copy()

        # Extract runtime_env as a dict if possible for easier merging
        runtime_env = init_kwargs.pop("runtime_env", {})
        if not isinstance(runtime_env, dict):
            # If it's already a RuntimeEnv object, convert to dict if possible
            if hasattr(runtime_env, "to_dict"):
                runtime_env = runtime_env.to_dict()
            else:
                # Fallback or assume it's already dict-like or we just treat as empty and warn?
                # For safety, let's try to keep it as is if we can't convert, but the merging logic below assumes a dict.
                runtime_env = {}

        # Merge env_vars
        current_env = runtime_env.get("env_vars", {})
        merged_env = default_env.copy()
        merged_env.update(current_env)
        runtime_env["env_vars"] = merged_env

        # Ensure uv packages are included if not already specified
        if "uv" not in runtime_env:
            runtime_env["uv"] = {"packages": pkgs}

        ray.init(address=ray_address, runtime_env=runtime_env, **init_kwargs)

    if "RAY_RUNTIME_ENV_HOOK" not in os.environ:
        os.environ["RAY_RUNTIME_ENV_HOOK"] = (
            "ray._private.runtime_env.uv_runtime_env_hook.hook"
        )


def _driver_process_inputs(container: Any, deps: list[str], s3_base_path: str) -> str:
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

    for dep_key in deps:
        try:
            local_path = str(get_nested_value(container, dep_key))
        except (KeyError, AttributeError):
            continue

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

        set_nested_value(container, dep_key, s3_uri)
    return s3_base


def _driver_process_outputs(container: Any, outs: list[str], s3_base: str) -> dict:
    original_local_outs = {}
    for out_key in outs:
        try:
            local_path = str(get_nested_value(container, out_key))
        except (KeyError, AttributeError):
            continue

        original_local_outs[out_key] = local_path
        s3_uri = f"{s3_base}/outs/{os.path.basename(local_path.rstrip('/'))}"

        logger.info(f"[Driver] Mapping output: {local_path} -> {s3_uri}")
        set_nested_value(container, out_key, s3_uri)
    return original_local_outs


def _driver_retrieve_outputs(container: Any, original_local_outs: dict):
    for out_key, local_path in original_local_outs.items():
        try:
            s3_uri = get_nested_value(container, out_key)
        except (KeyError, AttributeError):
            continue

        remote_hash = get_s3_hash(s3_uri)
        local_hash = calculate_path_hash(local_path)

        if remote_hash and remote_hash == local_hash:
            logger.info(
                f"[Driver] Output '{out_key}' matches current local content. Skipping download."
            )
        else:
            logger.info(f"[Driver] Downloading output '{out_key}' from S3...")
            s3_sync(s3_uri, local_path)
