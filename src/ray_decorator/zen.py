import os
from typing import Any, Callable

from omegaconf import OmegaConf

from .driver import (
    _driver_process_inputs,
    _driver_process_outputs,
    _driver_retrieve_outputs,
    _setup_ray_cluster,
)
from .utils import logger
from .worker import (
    _worker_process_inputs,
    _worker_process_outputs,
    _worker_upload_outputs,
)

try:
    from hydra_zen.wrapper import Zen as _BaseZen
except ImportError:
    _BaseZen = object


def ray_zen_worker(zen_inst: "RayZen", cfg: Any) -> Any:
    """Runs hydra-zen Zen.__call__ on Ray worker."""

    _worker_process_inputs(cfg, zen_inst.deps)
    s3_out_uris = _worker_process_outputs(cfg, zen_inst.outs)

    logger.info(f"[Worker] Starting execution of factory '{zen_inst.func.__name__}'...")
    cfg = OmegaConf.create(cfg)
    # Bypass RayZen.__call__ overrides and run the standard hydra-zen logic
    result = super(RayZen, zen_inst).__call__(cfg)
    logger.info(f"[Worker] Factory execution completed.")

    _worker_upload_outputs(cfg, s3_out_uris)
    return result


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
        if _BaseZen is object:
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

        s3_base = _driver_process_inputs(cfg_copy, self.deps, final_s3_base_path)
        original_local_outs = _driver_process_outputs(cfg_copy, self.outs, s3_base)

        _setup_ray_cluster(final_ray_address, self.ray_init_kwargs)

        logger.info(f"[Driver] Submitting factory '{self.func.__name__}' to Ray...")
        remote_wrapper = ray.remote(ray_zen_worker).options(
            **(self.ray_remote_kwargs or {})
        )
        result = ray.get(remote_wrapper.remote(self, cfg_copy))
        logger.info(f"[Driver] Remote execution completed.")

        _driver_retrieve_outputs(cfg_copy, original_local_outs)
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
