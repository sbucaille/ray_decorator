# Ray Decorator

`ray-decorator` is a library to offload function execution and configuration-driven tasks to a Ray cluster. It handles environment parity between local and remote environments by mapping local file paths to S3 URIs, performing deduplication via MD5 hashing, and ensuring dependencies are available on the worker node.

## Why this tool?

This tool was created to address friction when using **Ray** and **DVC** together in a unified pipeline.

In ML workflows, **DVC** is used to keep a local workspace up-to-date with data and parameters. However, training often requires a remote Ray cluster with GPUs. Balancing these two usually leads to complex staging commands in Ray job submissions, such as:

```bash
/bin/bash -c 'uv run dvc pull data/processed_dataset && \
uv run --script scripts/pipeline/train.py --dataset_path data/processed_dataset --model_output_path model/trained_model && \
uv run dvc add model/trained_model && \
uv run dvc push && \
aws s3 cp model/trained_model.dvc s3://my-bucket/models/'
```

Since DVC tracking of remote files (non-local to the workspace) is discouraged, it is preferable for dependencies and outputs to remain **local** from the perspective of the DVC pipeline.

`ray-decorator` handles this by:
1.  Copying local dependencies to S3.
2.  Syncing them to the remote Ray worker so the function runs on the Ray remote worker.
3.  Uploading the results back to S3 and then downloading them to the local machine.

Furthermore, it bypasses **Ray's `working_dir` size limits**. Including large datasets (e.g., 50GB of images) in a Ray `runtime_env` can lead to distribution overhead or failures. `ray-decorator` handles these as separate `deps`, syncing them directly to S3 and then to the worker, keeping the `working_dir` small.

The result is that **DVC can continue to track local dependencies and outputs** while the heavy lifting happens on the Ray cluster, without manual S3 management in scripts.

## Features

- **Distributed Execution**: Offload computations to a Ray cluster with a decorator.
- **S3 Path Mapping**: Detects local file paths in arguments, uploads them to S3, and maps them back to local paths on the Ray worker.
- **MD5 Deduplication**: Avoids redundant uploads/downloads by checking MD5 hashes.
- **Hydra & Hydra-Zen Integration**: Support for Hydra configurations and integration with `hydra-zen` via the `RayZen` wrapper.
- **Environment Parity**: Synchronizes local `uv` package distributions to the Ray cluster’s runtime environment.

## Installation

```bash
uv add ray-decorator
```

Requires `ray`, `awscli`, and optionally `hydra-zen`.

## Configuration

The following environment variables can be used to configure the default behavior:

- `RAY_ADDRESS`: The address of the Ray cluster (e.g., `ray://127.0.0.1:10001` or `auto`).
- `RAY_S3_BASE_PATH`: The base S3 bucket/path for storing dependencies and outputs (e.g., `s3://my-bucket/ray-jobs`).

## Usage Examples

### 1. Simple Function Arguments

Use `@ray_decorator` to offload a standard function. Specify `deps` for input paths and `outs` for output paths.

```python
import os
from ray_decorator import ray_decorator

@ray_decorator(
    deps=["data_dir"],
    outs=["output_dir"],
    ray_address="auto",
    s3_base_path="s3://my-bucket/jobs",
    ray_init_kwargs={"runtime_env": {"working_dir": os.getcwd()}},
    ray_remote_kwargs={"num_cpus": 2},
)
def process_data(data_dir: str, output_dir: str):
    print(f"Processing data from {data_dir}")
    os.makedirs(output_dir, exist_ok=True)
    with open(os.path.join(output_dir, "result.txt"), "w") as f:
        f.write("Done!")

if __name__ == "__main__":
    process_data(data_dir="./local_data", output_dir="./local_results")
```

### 2. Standard Hydra Integration

`ray-decorator` handles `DictConfig` objects automatically. You can specify nested paths in `deps` and `outs`.

```python
import hydra
from omegaconf import DictConfig
from ray_decorator import ray_decorator

@hydra.main(config_name="config", config_path=".", version_base=None)
@ray_decorator(
    deps=["config.data.path"],
    outs=["config.training.output_dir"],
)
def train(config: DictConfig):
    # This runs on Ray!
    print(f"Training on {config.data.path}")

if __name__ == "__main__":
    train()
```

### 3. Hydra-Zen Integration

For `hydra-zen` users, `ray_zen` is a drop-in replacement for `zen()` that ensures the entire instantiation and execution cycle happens on the Ray worker. This prevents heavy objects (like LLMs) from being instantiated on your local machine.

```python
from hydra_zen import builds, store
from ray_decorator import ray_zen
from transformers import AutoModel

@store(
    name="my_app",
    model=builds(AutoModel.from_pretrained, pretrained_model_name_or_path="bert-base-uncased"),
    data_dir="./data",
    output_dir="./outputs"
)
def task(model, data_dir, output_dir):
    # 'model' is instantiated ONLY on the Ray worker
    print(f"Model: {model.config.model_type}")

if __name__ == "__main__":
    store.add_to_hydra_store()
    
    ray_zen(
        task,
        deps=["data_dir"],
        outs=["output_dir"],
        ray_address="auto",
        s3_base_path="s3://my-bucket/zen",
    ).hydra_main(config_name="my_app", config_path=None, version_base=None)()
```

## How it Works

1. **Driver Side**:
   - Computes a stable "Run ID" based on the MD5 of all dependency paths.
   - Syncs local dependencies to S3 if the remote hash doesn't match.
   - Updates the configuration/arguments with S3 paths.
   - Initializes the Ray cluster with a `RuntimeEnv` containing the project code and required packages.
2. **Worker Side**:
   - Detects S3 paths in the configuration.
   - Syncs the required data from S3 to the worker's local storage.
   - Executes the function.
   - Syncs output files back to S3.
3. **Driver Side (Post-Execution)**:
   - Downloads the output files from S3 back to the original local paths.
