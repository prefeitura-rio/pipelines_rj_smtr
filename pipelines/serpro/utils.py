# -*- coding: utf-8 -*-
# import os
import subprocess

from prefect.engine.state import State

from pipelines.utils.secret import get_secret
from pipelines.utils.utils import log


def setup_serpro(secret_path: str = "radar_serpro"):
    data = get_secret(secret_path=secret_path)["setup.sh"]

    subprocess.run(["touch", "setup.sh"])
    with open("setup.sh", "w") as f:
        f.write(data)

    result = subprocess.run(["sh", "setup.sh"])

    if result.returncode == 0:
        log("setup.sh executou corretamente")
    else:
        raise Exception(f"Error executing setup.sh: {result.stderr}")

    return result


def handler_setup_serpro(obj, old_state: State, new_state: State) -> State:
    """
    State handler that will inject BD credentials into the environment.
    """
    if new_state.is_running():
        setup_serpro()
    return new_state
