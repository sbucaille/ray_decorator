import os
from importlib import metadata
from typing import Any, Dict

from .logging import logger
from .s3 import get_s3_hash, s3_sync, upload_hash
from .utils import (
    PathData,
    PathMatch,
    calculate_path_hash,
    get_nested_value,
    set_nested_value,
)


def _setup_ray_cluster(ray_address: str, ray_init_kwargs: dict | None = None):
    import ray

    if not ray.is_initialized():
        pkgs = [
            f"{d.metadata['Name']}=={d.version}"
            for d in metadata.distributions()
            # if d.metadata["Name"] != "ray-decorator"
        ]
        logger.ray_initialization(ray_address, len(pkgs))
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

        is_uv_run = "UV_RUN_RECURSION_DEPTH" in os.environ

        if is_uv_run:
            incompatible_keys = [k for k in ("uv", "pip") if k in runtime_env]
            if incompatible_keys:
                logger.log_runtime_env_keys_removed(incompatible_keys)
                for key in incompatible_keys:
                    runtime_env.pop(key, None)

        # Working dir packaging excludes `.venv`; workers need the same distributions
        # as the driver (torch, ray-decorator, etc.). Do not inject uv/pip under
        # `uv run`: Ray treats that combination as incompatible.
        if not is_uv_run and "uv" not in runtime_env and "pip" not in runtime_env:
            runtime_env["uv"] = {"packages": pkgs}

        try:
            with logger.status_ray_initialization(ray_address, len(pkgs)):
                ray.init(address=ray_address, runtime_env=runtime_env, **init_kwargs)
        except ConnectionError:
            if ray_address == "auto":
                logger.log_auto_address_fallback_to_local()
                ray.init(runtime_env=runtime_env, **init_kwargs)
            else:
                raise

    if "RAY_RUNTIME_ENV_HOOK" not in os.environ:
        os.environ["RAY_RUNTIME_ENV_HOOK"] = (
            "ray._private.runtime_env.uv_runtime_env_hook.hook"
        )


def _driver_process_inputs(
    container: Any, deps: list[str], s3_base_path: str
) -> Dict[str, PathMatch]:
    """
    Processes inputs for the driver by:
    - Calculate the hash of the local path.
    - Compare the hash with the hash of the remote path.
    - If the hashes match, it will skip the upload.
    - If the hashes do not match, it will upload the local path to S3.
    - It will set the remote path in the container.
    - It will return a dictionary of matches for the inputs.

    Args:
        container: The container to process inputs for.
        deps: The dependencies to process.
        s3_base_path: The base path to use for S3.

    Returns:
        A dictionary of matches for the inputs.

    Raises:
        FileNotFoundError: If an input is not found.
    """
    matches = {}
    with logger.status_driver_input_mapping_upload():
        for dep_key in deps:
            try:
                local_path = str(get_nested_value(container, dep_key))
            except (KeyError, AttributeError):
                continue

            if os.path.exists(local_path):
                is_dir = os.path.isdir(local_path)
                local_path_data = PathData(
                    path=local_path, is_dir=is_dir, location="local"
                )
                remote_path_data = PathData(
                    path=f"{s3_base_path}/deps/{os.path.basename(local_path_data.path.rstrip('/'))}",
                    is_dir=is_dir,
                    location="s3",
                )

                local_path_data.hash = calculate_path_hash(local_path_data.path)
                remote_path_data.hash = get_s3_hash(remote_path_data.path)

                if (
                    local_path_data.hash
                    and local_path_data.hash == remote_path_data.hash
                ):
                    logger.log_input_transfer_skip(
                        "Local", dep_key, "matches remote S3 hash"
                    )
                else:
                    logger.log_input_upload(dep_key)
                    s3_sync(local_path_data, remote_path_data)
                    upload_hash(remote_path_data.path, remote_path_data.hash)

                set_nested_value(container, dep_key, remote_path_data.path)
                path_match = PathMatch(local_path_data, remote_path_data)
                matches[dep_key] = path_match
            else:
                raise FileNotFoundError(
                    f"[Local] Input '{dep_key}' not found at {local_path}."
                )
    logger.log_inputs_mapping("Local", matches)
    return matches


def _driver_process_outputs(
    container: Any, outs: list[str], s3_base_path: str
) -> Dict[str, PathMatch]:
    """
    Processes outputs for the driver by:
    - Create a local path data object.
    - Create a remote path data object.
    - Set the remote path in the container.
    - Create a path match object.
    - Return a dictionary of matches for the outputs.

    Args:
        container: The container to process outputs for.
        outs: The outputs to process.
        s3_base_path: The base path to use for S3.

    Returns:
        A dictionary of matches for the outputs.

    Raises:
        FileNotFoundError: If an output is not found.
    """
    matches = {}
    for out_key in outs:
        try:
            local_path = str(get_nested_value(container, out_key))
        except (KeyError, AttributeError):
            continue
        local_path_data = PathData(path=local_path, is_dir=None, location="local")
        s3_uri = f"{s3_base_path}/outs/{os.path.basename(local_path.rstrip('/'))}"
        remote_path_data = PathData(path=s3_uri, is_dir=None, location="s3")

        set_nested_value(container, out_key, remote_path_data.path)
        path_match = PathMatch(local_path_data, remote_path_data)
        matches[out_key] = path_match
    logger.log_outputs_mapping("Local", matches)
    return matches


def _driver_retrieve_outputs(
    out_local_matches: Dict[str, PathMatch],
    remote_out_local_paths: Dict[str, PathMatch],
):
    """
    Retrieves outputs for the driver by:
    - Calculate the hash of the remote path.
    - Compare the hash with the hash of the local path.
    - If the hashes match, it will skip the download.
    - If the hashes do not match, it will download the remote path from S3.
    - It will set the local path in the container.
    - It will return a dictionary of matches for the outputs.

    Args:
        out_local_matches: A dictionary of matches for the outputs.
        remote_out_local_paths: A dictionary of matches for the outputs.

    Returns:
        A dictionary of matches for the outputs.

    Raises:
        FileNotFoundError: If an output is not found.
    """
    with logger.status_driver_output_download():
        for out_key in out_local_matches.keys():
            local_path_data = out_local_matches[out_key].local
            remote_path_data = remote_out_local_paths[out_key].remote

            remote_hash = get_s3_hash(remote_path_data.path)
            local_hash = calculate_path_hash(local_path_data.path)

            if remote_hash and remote_hash == local_hash:
                logger.log_output_transfer_skip(out_key)
            else:
                logger.log_output_download(remote_path_data, local_path_data)
                local_path_data.is_dir = remote_path_data.is_dir
                s3_sync(remote_path_data, local_path_data)
