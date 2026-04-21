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
from .utils import is_hydra_zen_available
from .worker import (
    _worker_process_inputs,
    _worker_process_outputs,
    _worker_upload_outputs,
)

if is_hydra_zen_available():
    from hydra_zen.wrapper import Zen as _BaseZen
    from omegaconf import OmegaConf
else:
    _BaseZen = object


def ray_zen_worker(
    zen_inst: "RayZen",
    cfg: Any,
    dep_path_matches: dict,
    out_path_matches: dict,
) -> Any:
    """Runs hydra-zen Zen.__call__ on Ray worker."""

    _worker_process_inputs(cfg, zen_inst.deps, dep_path_matches)
    worker_out_path_matches = _worker_process_outputs(
        cfg, zen_inst.outs, out_path_matches
    )

    logger.worker_execution_start(zen_inst.func.__name__)
    cfg = OmegaConf.create(cfg)
    # Bypass RayZen.__call__ overrides and run the standard hydra-zen logic
    result = super(RayZen, zen_inst).__call__(cfg)
    logger.log_worker_function_execution_complete(zen_inst.func.__name__)

    worker_out_path_matches = _worker_upload_outputs(
        zen_inst.outs, worker_out_path_matches
    )
    return result, worker_out_path_matches


class RayZen(_BaseZen):
    """
    Subclass of hydra_zen.wrapper.Zen that executes the wrapped function on a Ray cluster.
    """

    def __init__(
        self,
        func,
        *args,
        ray_address=None,
        s3_base_path=None,
        ray_init_kwargs=None,
        ray_remote_kwargs=None,
        deps=None,
        outs=None,
        **kwargs,
    ):
        if not is_hydra_zen_available():
            raise ImportError("The 'hydra-zen' package is required to use ray_zen.")
        super().__init__(func, *args, **kwargs)
        self.ray_address = ray_address
        self.s3_base_path = s3_base_path
        self.ray_init_kwargs = ray_init_kwargs
        self.ray_remote_kwargs = ray_remote_kwargs
        self.deps = deps or []
        self.outs = outs or []
        import functools

        functools.update_wrapper(self, getattr(self, "func", func))

    def __call__(self, __cfg):
        import ray

        if getattr(ray, "is_initialized", lambda: False) and not ray.is_initialized():
            pass  # let setup_ray_cluster handle it if needed

        # Duplicate config to avoid local side effects if needed.
        try:
            from omegaconf import OmegaConf

            cfg_copy = OmegaConf.to_container(__cfg, resolve=True)
        except ImportError:
            cfg_copy = __cfg

        final_ray_address = self.ray_address or os.environ.get("RAY_ADDRESS")
        final_s3_base_path = self.s3_base_path or os.environ.get("RAY_S3_BASE_PATH")

        if not final_ray_address or not final_s3_base_path:
            raise ValueError("Ray address and S3 base path must be provided.")

        final_s3_base_path = get_s3_base_path(final_s3_base_path, self.deps, cfg_copy)
        dep_path_matches = _driver_process_inputs(
            cfg_copy, self.deps, final_s3_base_path
        )
        out_path_matches = _driver_process_outputs(
            cfg_copy, self.outs, final_s3_base_path
        )

        _setup_ray_cluster(final_ray_address, self.ray_init_kwargs)

        logger.log_submit_factory(self.func.__name__)
        remote_wrapper = ray.remote(ray_zen_worker).options(
            **(self.ray_remote_kwargs or {})
        )
        with logger.status_driver_waiting_remote_execution(self.func.__name__):
            result, worker_out_path_matches = ray.get(
                remote_wrapper.remote(
                    self, cfg_copy, dep_path_matches, out_path_matches
                )
            )
        logger.remote_execution_completed("Local")

        _driver_retrieve_outputs(out_path_matches, worker_out_path_matches)
        return result


def ray_zen(
    __func: Callable,
    *,
    ray_address: str | None = None,
    s3_base_path: str | None = None,
    ray_init_kwargs: dict | None = None,
    ray_remote_kwargs: dict | None = None,
    deps: list[str] | None = None,
    outs: list[str] | None = None,
    **kwargs,
) -> RayZen:
    """
    Acts as hydra_zen.zen but executes the function on a Ray cluster.
    """
    return RayZen(
        __func,
        ray_address=ray_address,
        s3_base_path=s3_base_path,
        ray_init_kwargs=ray_init_kwargs,
        ray_remote_kwargs=ray_remote_kwargs,
        deps=deps,
        outs=outs,
        **kwargs,
    )
