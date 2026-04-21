"""
Loading `ray_zen` / `RayZen` pulls in hydra-zen and omegaconf. Users who only need
`ray_decorator` should not pay that cost (or hit missing optional deps); we lazy-load
the zen submodule on first access (PEP 562).
"""

from .core import ray_decorator

__all__ = ["ray_decorator", "ray_zen", "RayZen"]


def __getattr__(name: str):
    if name in ("RayZen", "ray_zen"):
        from .utils import is_hydra_zen_available

        if not is_hydra_zen_available():
            raise ImportError(
                "RayZen and ray_zen require optional dependencies (hydra-zen). "
                "Install with: uv add 'ray-decorator[zen]' "
                "or pip install 'ray-decorator[zen]'."
            )
        from .zen import RayZen, ray_zen

        return RayZen if name == "RayZen" else ray_zen
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
