import time

import ray
from rich.console import Console


@ray.remote
def remote_task_with_spinner(name):
    # force_terminal=True is key for the spinner to show up
    console = Console(force_terminal=True, width=100)

    # Removed 'transient' - it's not a valid arg for .status()
    with console.status(f"[bold green]Working on {name}...", spinner="dots"):
        for i in range(5):
            time.sleep(1)
            if i == 2:
                console.log(f"[yellow]Checkpoint reached[/yellow] for {name}")

    return f"Task {name} complete"


ray.init()
print(ray.get(remote_task_with_spinner.remote("Dataset-A")))
