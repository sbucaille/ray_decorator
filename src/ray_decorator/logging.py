import logging
from collections.abc import Iterable
from typing import Any, Literal

from rich.console import Console
from rich.spinner import SPINNERS
from rich.status import Status

from .utils import PathMatch

# Define your custom animation
SPINNERS["to_remote"] = {
    "interval": 100,  # Speed in milliseconds
    "frames": [
        "▹▹▹▹▹☁️",
        "▸▹▹▹▹☁️",
        "▹▸▹▹▹☁️",
        "▹▹▸▹▹☁️",
        "▹▹▹▸▹☁️",
        "▹▹▹▹▸☁️",
    ],  # The sequence of frames
}

SPINNERS["from_remote"] = {
    "interval": 100,  # Speed in milliseconds
    "frames": [
        "◃◃◃◃◃☁️",
        "◃◃◃◃◂☁️",
        "◃◃◃◂◃☁️",
        "◃◃◂◃◃☁️",
        "◃◂◃◃◃☁️",
        "◂◃◃◃◃☁️",
    ],  # The sequence of frames
}
Component = Literal["Local", "Remote", "S3 Sync"]


class RayDecoratorLogger:
    """Rich-first logger facade used across the runtime pipeline."""

    def __init__(self, name: str = "ray_decorator", level: int = logging.INFO):
        # Ray worker streams are often non-interactive; force ANSI colors so styled
        # messages are preserved, while `_emit` keeps line endings clean.
        self.console = Console(force_terminal=True)
        self.logger = logging.getLogger(name)
        self.logger.setLevel(level)
        self.logger.propagate = False
        if not self.logger.handlers:
            self.logger.addHandler(logging.NullHandler())

    def _emit(self, message: str, *, style: str | None = None):
        clean_message = message.rstrip("\n")
        self.console.print(clean_message, style=style)

    def debug(self, message: str):
        if self.logger.isEnabledFor(logging.DEBUG):
            self._emit(message)

    def info(self, message: str):
        if self.logger.isEnabledFor(logging.INFO):
            self._emit(message)

    def warning(self, message: str):
        if self.logger.isEnabledFor(logging.WARNING):
            self._emit(message, style="yellow")

    def error(self, message: str):
        if self.logger.isEnabledFor(logging.ERROR):
            self._emit(message, style="bold red")

    def exception(self, message: str):
        self.error(message)

    def status(self, message: str, spinner: str | None = None) -> Status:
        return self.console.status(message, spinner=spinner or "dots")

    def stable_run_id(self, run_id: str):
        self.info(f"[Local] Using stable Run ID: {run_id}")

    def status_driver_input_mapping_upload(self) -> Status:
        return self.status(
            "[Local] Mapping and uploading inputs...", spinner="to_remote"
        )

    def status_driver_output_download(self) -> Status:
        return self.status("[Local] Downloading outputs...", spinner="from_remote")

    def status_driver_waiting_remote_execution(self, func_name: str) -> Status:
        return self.status(f"Running '{func_name}' remotely...", spinner="dots")

    def ray_initialization(self, ray_address: str, package_count: int) -> Status:
        return self.status(
            f"[Local] Initializing Ray at {ray_address} with {package_count} packages..."
        )

    def info_worker_input_download(self):
        self.info("[Remote] Downloading inputs...")

    def info_worker_function_call(self, func_name: str):
        self.info(f"[Remote] Calling '{func_name}'...")

    def info_worker_output_upload(self):
        self.info("[Remote] Uploading outputs...")

    def s3_sync(self, src: Any, dst: Any):
        self.info(f"[S3 Sync] Syncing '{src}' to '{dst}'...")

    def log_runtime_env_keys_removed(self, keys: Iterable[str]):
        joined_keys = ", ".join(keys)
        self.warning(
            "[Local] Running under `uv run`; removing incompatible "
            f"runtime_env keys: {joined_keys}."
        )

    def log_auto_address_fallback_to_local(self):
        self.warning(
            "[Local] No running Ray instance found for address='auto'. "
            "Starting a local Ray instance instead."
        )

    def log_submit_function(
        self,
        component: Component,
        func_name: str,
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
    ):
        arg_lines = [f"    {i}: {repr(arg)}" for i, arg in enumerate(args)]
        kwarg_lines = [f"    {k}: {repr(v)}" for k, v in kwargs.items()]
        msg_lines = [
            f"[{component}] Submitting '{func_name}' to Ray cluster with:",
            "  args:",
            *arg_lines,
            "  kwargs:",
            *kwarg_lines,
            "...",
        ]
        self.info("\n".join(msg_lines))

    def log_submit_factory(self, func_name: str):
        self.info(f"[Local] Submitting factory '{func_name}' to Ray...")

    def bound_arguments(self, args: tuple[Any, ...], kwargs: dict[str, Any]):
        self.info(f"[Local] Bound args: {args}, kwargs: {kwargs}")

    def remote_execution_completed(self, component: Component = "Local"):
        self.info(f"[{component}] Remote execution completed.")

    def log_inputs_mapping(self, component: Component, matches: dict[str, PathMatch]):
        input_lines = []
        for dep_key, match in matches.items():
            input_lines.append(f"  {dep_key}: {match}")
        self.info(f"[{component}] Mapping inputs:\n" + "\n".join(input_lines))

    def log_input_transfer_skip(self, component: Component, dep_key: str, reason: str):
        self.info(f"[{component}] Input '{dep_key}' {reason}. Skipping transfer.")

    def log_input_upload(self, dep_key: str):
        self.info(f"[Local] Uploading input '{dep_key}' to S3...")

    def log_input_download(self, dep_key: str):
        self.info(f"[Remote] Syncing input '{dep_key}' from S3...")

    def log_processed_inputs(self, component: Component, payload: Any):
        self.info(f"[{component}] Processed inputs: {payload}")

    def log_outputs_mapping(self, component: Component, matches: dict[str, PathMatch]):
        output_lines = []
        for out_key, match in matches.items():
            output_lines.append(f"  {out_key}: {match}")
        self.info(f"[{component}] Mapping outputs:\n" + "\n".join(output_lines))

    def log_output_transfer_skip(self, out_key: str):
        self.info(
            f"[Local] Output '{out_key}' matches current local content. Skipping download."
        )

    def log_output_download(self, remote: Any, local: Any):
        self.info(f"[Local] Downloading {remote} -> {local}")

    def output_skip_upload(self, out_key: str):
        self.info(
            f"[Remote] Output '{out_key}' matches remote S3 hash. Skipping upload."
        )

    def output_upload(self, out_key: str):
        self.info(f"[Remote] Uploading output '{out_key}' to S3...")

    def worker_execution_start(self, func_name: str):
        self.info(f"[Remote] Starting execution of '{func_name}'...")

    def log_worker_function_execution_complete(self, func_name: str):
        self.info(f"[Remote] Execution of '{func_name}' completed.")

    def worker_factory_execution_start(self, func_name: str):
        self.info(f"[Remote] Starting execution of factory '{func_name}'...")

    def worker_factory_execution_complete(self):
        self.info("[Remote] Factory execution completed.")

    def log_worker_function_arguments(
        self, args: tuple[Any, ...], kwargs: dict[str, Any]
    ):
        arg_lines = [f"  arg[{idx}]: {arg!r}" for idx, arg in enumerate(args)]
        kwarg_lines = [f"  kwarg '{k}': {v!r}" for k, v in kwargs.items()]
        self.info("[Remote] Function arguments:\n" + "\n".join(arg_lines + kwarg_lines))

    def log_worker_remote_output_paths(self, output_matches: dict[str, Any]):
        self.info(
            "[Remote] Remote output paths:\n"
            + "\n".join([f"  {k}: {v}" for k, v in output_matches.items()])
        )


logger = RayDecoratorLogger()
