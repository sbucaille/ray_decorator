import json
import os

import hydra
from omegaconf import DictConfig, OmegaConf

from ray_decorator import ray_decorator


@hydra.main(config_name="main", config_path=".", version_base=None)
@ray_decorator(
    deps=["config.data_module.data_dir"],
    outs=["config.output_dir"],
    ray_address="auto",
    s3_base_path="s3://lassonde",
    ray_init_kwargs={
        "runtime_env": {
            "working_dir": os.getcwd(),
            "py_modules": ["./src/ray_decorator"],
        }
    },
    ray_remote_kwargs={"num_cpus": 2},
)
def main(config: DictConfig) -> None:
    model = hydra.utils.instantiate(config.model)
    model.save_pretrained(config.output_dir)
    with open(os.path.join(config.output_dir, "data.json"), "w") as f:
        json.dump(OmegaConf.to_container(config), f)


if __name__ == "__main__":
    main()
