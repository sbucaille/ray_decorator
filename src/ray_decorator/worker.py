import os
from typing import Any, Callable, Dict

from .logging import logger
from .s3 import get_s3_hash, s3_sync, upload_hash
from .utils import (
    PathData,
    PathMatch,
    calculate_path_hash,
    set_nested_value,
)


def _worker_process_inputs(
    container: Any, deps: list[str], dep_path_matches: Dict[str, PathMatch]
):
    """
    Processes inputs for the worker by:
    - Get the remote path data object.
    - Create a local path data object.
    - Compare the hash of the local path with the hash of the remote path.
    - If the hashes match, it will skip the download.
    - If the hashes do not match, it will download the remote path from S3.
    - Set the local path in the container.
    - Create a path match object.

    Args:
        container: The container to process inputs for.
        deps: The dependencies to process.
        dep_path_matches: A dictionary of matches for the inputs.

    """
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
        remote_path_data.hash = get_s3_hash(remote_path_data.path)
        local_path_data.hash = calculate_path_hash(local_path_data.path)

        if remote_path_data.hash and remote_path_data.hash == local_path_data.hash:
            logger.log_input_transfer_skip("Remote", dep_key, "matches remote hash")
        else:
            logger.log_input_download(dep_key)
            s3_sync(remote_path_data, local_path_data)

        set_nested_value(container, dep_key, local_path_data.path)
    logger.log_inputs_mapping(
        "Remote", {dep_key: PathMatch(local_path_data, remote_path_data)}
    )
    logger.log_processed_inputs("Remote", container)


def _worker_process_outputs(
    container: Any, outs: list[str], out_path_matches: Dict[str, PathMatch]
):
    """
    Processes outputs for the worker by:
    - Get the remote path data object.
    - Create a local path data object.
    - Set the local path in the container.

    Args:
        container: The container to process outputs for.
        outs: The outputs to process.
        out_path_matches: A dictionary of matches for the outputs.

    Returns:
        A dictionary of matches for the outputs.

    Raises:
        FileNotFoundError: If an output is not found.
    """
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

        set_nested_value(container, out_key, local_path_data.path)
        worker_out_path_matches[out_key] = PathMatch(
            local=local_path_data, remote=remote_path_data
        )
    logger.log_outputs_mapping("Remote", worker_out_path_matches)
    return worker_out_path_matches


def _worker_upload_outputs(outs: list[str], worker_out_path_matches: dict):
    for out_key in outs:
        local_path_data = worker_out_path_matches[out_key].local
        remote_path_data = worker_out_path_matches[out_key].remote

        if os.path.exists(local_path_data.path):
            local_path_data.hash = calculate_path_hash(local_path_data.path)
            remote_path_data.hash = get_s3_hash(remote_path_data.path)

            if local_path_data.hash == remote_path_data.hash:
                logger.output_skip_upload(out_key)
            else:
                logger.output_upload(out_key)
                local_path_data.is_dir = os.path.isdir(local_path_data.path)
                remote_path_data.is_dir = local_path_data.is_dir
                s3_sync(local_path_data, remote_path_data)
                upload_hash(remote_path_data.path, local_path_data.hash)
                remote_path_data.hash = get_s3_hash(remote_path_data.path)
        else:
            raise FileNotFoundError(
                f"[Remote] Output '{out_key}' not found at {local_path_data.path} after execution."
            )
    return worker_out_path_matches


def worker_wrapper(
    func: Callable,
    args: tuple,
    kwargs: dict,
    deps: list[str],
    outs: list[str],
    dep_path_matches: Dict[str, PathMatch],
    out_path_matches: Dict[str, PathMatch],
) -> Any:
    """
    Wraps a function by processing inputs and outputs on the worker.
    It will:
    - Process inputs : download inputs from S3 to local.
    - Process outputs : set local paths in the container where the function will create them.
    - Execute the function.
    - Upload outputs : upload outputs from local to S3.
    - Return the result and the output matches.

    Args:
        func: The function to wrap.
        args: The arguments to pass to the function.
        kwargs: The keyword arguments to pass to the function.
        deps: The dependencies to process.
        outs: The outputs to process.
        dep_path_matches: A dictionary of matches for the inputs.
        out_path_matches: A dictionary of matches for the outputs.

    Returns:
        The result of the function and the output matches.

    Raises:
        FileNotFoundError: If an output is not found.
        Exception: If an error occurs while executing the function.
    """
    _worker_process_inputs(kwargs, deps, dep_path_matches)
    worker_out_path_matches = _worker_process_outputs(kwargs, outs, out_path_matches)

    logger.worker_execution_start(func.__name__)
    logger.log_worker_function_arguments(args, kwargs)
    result = func(*args, **kwargs)
    logger.log_worker_function_execution_complete(func.__name__)

    worker_out_path_matches = _worker_upload_outputs(outs, worker_out_path_matches)
    logger.log_worker_remote_output_paths(worker_out_path_matches)
    return result, worker_out_path_matches
