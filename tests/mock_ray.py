import os
import shutil
import sys
import types
from typing import Any, Callable


class MockRay:
    """
    A simple Mock Ray instance to simulate remote execution locally.
    """

    def __init__(self):
        self.initialized = False

    def init(self, *args, **kwargs):
        self.initialized = True

    def shutdown(self):
        self.initialized = False

    def is_initialized(self):
        return self.initialized

    def remote(self, func: Callable):
        class RemoteFunction:
            def __init__(self, f):
                self.f = f
                self._options = {}

            def options(self, **kwargs):
                self._options.update(kwargs)
                return self

            def remote(self, *args, **kwargs):
                return self.f(*args, **kwargs)

        return RemoteFunction(func)

    def get(self, object_ref: Any) -> Any:
        return object_ref


def mock_s3_sync(src: str, dst: str, is_dir: bool = False):
    """A mock s3_sync that performs local copies for testing."""
    if src.startswith("s3://") or dst.startswith("s3://"):
        pass
    else:
        if os.path.isdir(src):
            if os.path.exists(dst):
                shutil.rmtree(dst)
            shutil.copytree(src, dst)
        else:
            os.makedirs(os.path.dirname(os.path.abspath(dst)), exist_ok=True)
            shutil.copy2(src, dst)


def patch_ray(mock_s3=True):
    """Utility to patch the ray module and submodules globally for testing."""
    mock_ray_inst = MockRay()

    # Create a real module object so 'from ray.runtime_env import ...' works
    ray_mod = types.ModuleType("ray")
    ray_mod.init = mock_ray_inst.init
    ray_mod.shutdown = mock_ray_inst.shutdown
    ray_mod.is_initialized = mock_ray_inst.is_initialized
    ray_mod.remote = mock_ray_inst.remote
    ray_mod.get = mock_ray_inst.get

    # Mock submodules
    runtime_env_mod = types.ModuleType("ray.runtime_env")
    runtime_env_mod.RuntimeEnv = lambda **kwargs: kwargs

    sys.modules["ray"] = ray_mod
    sys.modules["ray.runtime_env"] = runtime_env_mod

    if mock_s3:
        try:
            import ray_decorator.utils

            # ray_decorator.utils.s3_sync = mock_s3_sync
            ray_decorator.utils.get_s3_hash = lambda x: "mock_hash"
            ray_decorator.utils.upload_hash = lambda x, y: None
            ray_decorator.utils.is_aws_available = lambda: True
        except ImportError:
            pass

    return ray_mod


if __name__ == "__main__":
    ray = patch_ray()

    @ray.remote
    def add(x, y):
        return x + y

    ref = add.remote(1, 2)
    print(f"Result: {ray.get(ref)}")  # Should print 3
