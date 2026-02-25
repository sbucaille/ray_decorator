import os
import sys

# 1. Patch ray BEFORE importing any ray_decorator modules
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from tests.mock_ray import patch_ray

patch_ray()

# 2. Now import your code
from ray_decorator import ray_decorator


@ray_decorator(
    deps=["test_in"],
    outs=["test_out"],
    ray_address="local",
    s3_base_path="s3://lassonde/mock_output",
)
def my_func(test_in: str, test_out: str):
    print(f"Worker running: creating {test_out}")
    os.makedirs(test_out, exist_ok=True)
    with open(os.path.join(test_out, "hello.txt"), "w") as f:
        f.write("hello from mock worker")
    return "Success"


if __name__ == "__main__":
    # Ensure a local directory exists for the "remote" worker to use
    res = my_func(test_in="mock_input.txt", test_out="mock_output")
    print(f"Final Result: {res}")
