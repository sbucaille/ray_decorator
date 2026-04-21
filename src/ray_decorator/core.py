import functools
import inspect
import os
from typing import Any, Callable

from .driver import (
    _driver_process_inputs,
    _driver_process_outputs,
    _driver_retrieve_outputs,
    _setup_ray_cluster,
)
from .logging import logger
from .s3 import get_s3_base_path
from .utils import is_ray_available
from .worker import worker_wrapper


def ray_decorator(
    deps: list[str],
    outs: list[str],
    ray_address: str | None = None,
    s3_base_path: str | None = None,
    ray_init_kwargs: dict | None = None,
    ray_remote_kwargs: dict | None = None,
) -> Callable:
    """
    Standard decorator to offload execution to Ray.
    """

    def decorator(func: Callable) -> Callable:
        if not is_ray_available():
            raise ValueError("The 'ray' package is required in your environment.")

        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            import ray

            # Bind arguments to the function
            sig = inspect.signature(func)
            bound = sig.bind(*args, **kwargs)
            bound.apply_defaults()
            bound_kwargs = bound.arguments

            # Get final Ray address and S3 base path
            final_ray_address = ray_address or os.environ.get("RAY_ADDRESS")
            final_s3_base_path = s3_base_path or os.environ.get("RAY_S3_BASE_PATH")
            if not final_ray_address or not final_s3_base_path:
                raise ValueError("Ray address and S3 base path must be provided.")

            final_s3_base_path = get_s3_base_path(
                final_s3_base_path, deps, bound_kwargs
            )

            # Process inputs and outputs
            dep_path_matches = _driver_process_inputs(
                bound_kwargs, deps, final_s3_base_path
            )
            out_path_matches = _driver_process_outputs(
                bound_kwargs, outs, final_s3_base_path
            )

            _setup_ray_cluster(final_ray_address, ray_init_kwargs)

            logger.log_submit_function("Local", func.__name__, args, kwargs)
            with logger.status_driver_waiting_remote_execution(func.__name__):
                remote_wrapper = ray.remote(worker_wrapper).options(
                    **(ray_remote_kwargs or {})
                )
                # Positional args are not provided because they are handled in kwargs already
                result, worker_out_path_matches = ray.get(
                    remote_wrapper.remote(
                        func,
                        (),
                        bound_kwargs,
                        deps,
                        outs,
                        dep_path_matches,
                        out_path_matches,
                    )
                )
            logger.remote_execution_completed("Local")
            _driver_retrieve_outputs(out_path_matches, worker_out_path_matches)
            return result

        return wrapper

    return decorator
