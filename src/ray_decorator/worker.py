import os
from typing import Any, Callable, Dict

from .utils import (
    PathData,
    PathMatch,
    calculate_path_hash,
    get_s3_hash,
    logger,
    s3_sync,
    set_nested_value,
    upload_hash,
)


def _worker_process_inputs(
    container: Any, deps: list[str], dep_path_matches: Dict[str, PathMatch]
):
    worker_run_dir = os.getcwd()
    for dep_key in deps:
        remote_path_data = dep_path_matches[dep_key].remote

        basename = os.path.basename(remote_path_data.path.rstrip("/"))
        local_worker_path = os.path.join(worker_run_dir, "deps", basename)
        local_path_data = PathData(
            path=local_worker_path,
            location="local",
            is_dir=remote_path_data.is_dir,
        )
        logger.info(
            f"[Worker] Mapping input: {remote_path_data.path} -> {local_path_data.path}"
        )
        remote_path_data.hash = get_s3_hash(remote_path_data.path)
        local_path_data.hash = calculate_path_hash(local_path_data.path)

        if remote_path_data.hash and remote_path_data.hash == local_path_data.hash:
            logger.info(
                f"[Worker] Input '{dep_key}' matches remote hash. Skipping download."
            )
        else:
            logger.info(f"[Worker] Syncing input '{dep_key}' from S3...")
            s3_sync(remote_path_data, local_path_data)

        set_nested_value(container, dep_key, local_path_data.path)


def _worker_process_outputs(
    container: Any, outs: list[str], out_path_matches: Dict[str, PathMatch]
) -> Dict[str, PathMatch]:
    worker_run_dir = os.getcwd()
    worker_out_path_matches = {}
    for out_key in outs:
        remote_path_data = out_path_matches[out_key].remote

        basename = os.path.basename(remote_path_data.path.rstrip("/"))
        local_worker_path = os.path.join(worker_run_dir, "outs", basename)
        local_path_data = PathData(
            path=local_worker_path,
            location="local",
            is_dir=remote_path_data.is_dir,
        )

        logger.info(
            f"[Worker] Mapping output: {remote_path_data.path} -> {local_path_data.path}"
        )
        set_nested_value(container, out_key, local_path_data.path)
        worker_out_path_matches[out_key] = PathMatch(
            local=local_path_data, remote=remote_path_data
        )
    return worker_out_path_matches


def _worker_upload_outputs(outs: list[str], worker_out_path_matches: dict):
    for out_key in outs:
        local_path_data = worker_out_path_matches[out_key].local
        remote_path_data = worker_out_path_matches[out_key].remote

        if os.path.exists(local_path_data.path):
            local_path_data.hash = calculate_path_hash(local_path_data.path)
            remote_path_data.hash = get_s3_hash(remote_path_data.path)

            if local_path_data.hash == remote_path_data.hash:
                logger.info(
                    f"[Worker] Output '{out_key}' matches remote S3 hash. Skipping upload."
                )
            else:
                logger.info(f"[Worker] Uploading output '{out_key}' to S3...")
                local_path_data.is_dir = os.path.isdir(local_path_data.path)
                remote_path_data.is_dir = local_path_data.is_dir
                s3_sync(local_path_data, remote_path_data)
                upload_hash(remote_path_data.path, local_path_data.hash)
                remote_path_data.hash = get_s3_hash(remote_path_data.path)
        else:
            raise FileNotFoundError(
                f"[Worker] Output '{out_key}' not found at {local_path_data.path} after execution."
            )
    return worker_out_path_matches


def worker_wrapper(
    func: Callable,
    args: tuple,
    kwargs: dict,
    deps: list[str],
    outs: list[str],
    dep_path_matches: dict,
    out_path_matches: dict,
) -> Any:
    """Runs standard function on Ray worker."""
    _worker_process_inputs(kwargs, deps, dep_path_matches)
    worker_out_path_matches = _worker_process_outputs(kwargs, outs, out_path_matches)

    logger.info(f"[Worker] Starting execution of '{func.__name__}'...")
    logger.info(f"[Worker] Function arguments: args={args}, kwargs={kwargs}")
    result = func(*args, **kwargs)
    logger.info(f"[Worker] Execution of '{func.__name__}' completed.")

    worker_out_path_matches = _worker_upload_outputs(outs, worker_out_path_matches)
    logger.info(f"[Worker] Remote output paths: {worker_out_path_matches}")
    return result, worker_out_path_matches
