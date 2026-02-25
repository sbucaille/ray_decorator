import json
import os
from typing import Any, Dict

from hydra_zen import builds, store
from transformers import AutoModel, PreTrainedModel


@store(
    name="main",
    data_module={"data_dir": "./examples/data"},
    model=builds(
        AutoModel.from_pretrained, pretrained_model_name_or_path="bert-base-uncased"
    ),
    output_dir="./examples/outputs",
)
def main(data_module: Dict[str, Any], model: PreTrainedModel, output_dir: str) -> None:
    print(data_module)
    model.save_pretrained(output_dir)
    os.makedirs(output_dir, exist_ok=True)
    with open(os.path.join(output_dir, "data.json"), "w") as f:
        json.dump(data_module, f)


if __name__ == "__main__":
    from ray_decorator import ray_zen

    store.add_to_hydra_store()

    ray_zen(
        main,
        deps=["data_module.data_dir"],
        outs=["output_dir"],
        ray_address="auto",
        s3_base_path="s3://lassonde",
    ).hydra_main(config_name="main", config_path=None, version_base=None)
