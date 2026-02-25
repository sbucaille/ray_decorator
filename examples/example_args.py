import json
import os

from ray_decorator import ray_decorator


@ray_decorator(
    deps=["data_dir"],
    outs=["output_dir"],
    ray_address="auto",
    s3_base_path="s3://lassonde",
    working_dir=os.getcwd(),
)
def main(data_dir: str, output_dir: str) -> None:
    print(data_dir)
    os.makedirs(output_dir, exist_ok=True)
    with open(os.path.join(output_dir, "data.json"), "w") as f:
        json.dump(data_dir, f)


if __name__ == "__main__":
    main(data_dir="./examples/data_dir", output_dir="./examples/outputs")
