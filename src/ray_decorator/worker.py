import os
from typing import Any, Callable

from .utils import (
    calculate_path_hash,
    get_nested_value,
    get_s3_hash,
    logger,
    s3_sync,
    set_nested_value,
    upload_hash,
)


def _worker_process_inputs(container: Any, deps: list[str]):
    worker_run_dir = os.getcwd()
    for dep_key in deps:
        try:
            s3_uri = get_nested_value(container, dep_key)
            if not isinstance(s3_uri, str) or not s3_uri.startswith("s3://"):
                continue
        except (KeyError, AttributeError):
            continue

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

        set_nested_value(container, dep_key, local_worker_path)


def _worker_process_outputs(container: Any, outs: list[str]) -> dict:
    worker_run_dir = os.getcwd()
    s3_out_uris = {}
    for out_key in outs:
        try:
            s3_uri = get_nested_value(container, out_key)
            if not isinstance(s3_uri, str) or not s3_uri.startswith("s3://"):
                continue
        except (KeyError, AttributeError):
            continue

        s3_out_uris[out_key] = s3_uri
        basename = os.path.basename(s3_uri.rstrip("/"))
        local_worker_path = os.path.join(worker_run_dir, "outs", basename)

        logger.info(f"[Worker] Mapping output: {s3_uri} -> {local_worker_path}")
        set_nested_value(container, out_key, local_worker_path)
    return s3_out_uris


def _worker_upload_outputs(container: Any, s3_out_uris: dict):
    for out_key, s3_uri in s3_out_uris.items():
        try:
            local_path = get_nested_value(container, out_key)
        except (KeyError, AttributeError):
            continue

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


def worker_wrapper(
    func: Callable, args: tuple, kwargs: dict, deps: list[str], outs: list[str]
) -> Any:
    """Runs standard function on Ray worker."""
    _worker_process_inputs(kwargs, deps)
    s3_out_uris = _worker_process_outputs(kwargs, outs)

    logger.info(f"[Worker] Starting execution of '{func.__name__}'...")
    logger.info(f"[Worker] Function arguments: args={args}, kwargs={kwargs}")
    result = func(*args, **kwargs)
    logger.info(f"[Worker] Execution of '{func.__name__}' completed.")

    _worker_upload_outputs(kwargs, s3_out_uris)
    return result
