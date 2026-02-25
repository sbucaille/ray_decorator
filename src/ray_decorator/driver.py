import hashlib
import os
from importlib import metadata
from typing import Any, Dict

from .utils import (
    PathData,
    PathMatch,
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


def _driver_process_inputs(
    container: Any, deps: list[str], s3_base_path: str
) -> Dict[str, PathMatch]:
    matches = {}
    for dep_key in deps:
        try:
            local_path = str(get_nested_value(container, dep_key))
        except (KeyError, AttributeError):
            continue

        if os.path.exists(local_path):
            is_dir = os.path.isdir(local_path)
            local_path_data = PathData(path=local_path, is_dir=is_dir, location="local")
            remote_path_data = PathData(
                path=f"{s3_base_path}/deps/{os.path.basename(local_path_data.path.rstrip('/'))}",
                is_dir=is_dir,
                location="s3",
            )

            logger.info(
                f"[Driver] Mapping input: {local_path_data.path} -> {remote_path_data.path}"
            )

            local_path_data.hash = calculate_path_hash(local_path_data.path)
            remote_path_data.hash = get_s3_hash(remote_path_data.path)

            if local_path_data.hash and local_path_data.hash == remote_path_data.hash:
                logger.info(
                    f"[Driver] Input '{dep_key}' matches remote S3 hash. Skipping upload."
                )
            else:
                logger.info(f"[Driver] Uploading input '{dep_key}' to S3...")
                s3_sync(local_path_data, remote_path_data)
                upload_hash(remote_path_data.path, remote_path_data.hash)

            set_nested_value(container, dep_key, remote_path_data.path)
            path_match = PathMatch(local_path_data, remote_path_data)
            matches[dep_key] = path_match
        else:
            raise FileNotFoundError(
                f"[Driver] Input '{dep_key}' not found at {local_path}."
            )
    return matches


def _driver_process_outputs(
    container: Any, outs: list[str], s3_base_path: str
) -> Dict[str, PathMatch]:
    matches = {}
    for out_key in outs:
        try:
            local_path = str(get_nested_value(container, out_key))
        except (KeyError, AttributeError):
            continue
        local_path_data = PathData(path=local_path, is_dir=None, location="local")
        s3_uri = f"{s3_base_path}/outs/{os.path.basename(local_path.rstrip('/'))}"
        remote_path_data = PathData(path=s3_uri, is_dir=None, location="s3")

        logger.info(
            f"[Driver] Mapping output: {local_path_data.path} -> {remote_path_data.path}"
        )
        set_nested_value(container, out_key, remote_path_data.path)
        path_match = PathMatch(local_path_data, remote_path_data)
        matches[out_key] = path_match
    return matches


def _driver_retrieve_outputs(
    out_local_matches: Dict[str, PathMatch],
    remote_out_local_paths: Dict[str, PathMatch],
):
    for out_key in out_local_matches.keys():
        local_path_data = out_local_matches[out_key].local
        remote_path_data = remote_out_local_paths[out_key].remote

        remote_hash = get_s3_hash(remote_path_data.path)
        local_hash = calculate_path_hash(local_path_data.path)

        if remote_hash and remote_hash == local_hash:
            logger.info(
                f"[Driver] Output '{out_key}' matches current local content. Skipping download."
            )
        else:
            logger.info(f"[Driver] Downloading output '{out_key}' from S3...")
            logger.info(f"[Driver] Remote path: {remote_path_data.path}")
            logger.info(f"[Driver] Local path: {local_path_data.path}")
            local_path_data.is_dir = remote_path_data.is_dir
            s3_sync(remote_path_data, local_path_data)
