from prefect.engine.state import State
import os

from pipelines.utils.secret import get_secret
from pipelines.utils.utils import log

def setup_serpro(secret_path:str='radar_serpro'):
    data = get_secret(secret_path=secret_path)['setup.sh']
    log('Got Secret')
    os.popen('touch setup.sh')
    with open('setup.sh','w') as f:
        f.write(data)
    return os.popen("sh setup.sh")


def handler_setup_serpro(obj, old_state: State, new_state: State) -> State:
    """
    State handler that will inject BD credentials into the environment.
    """
    if new_state.is_running():
        setup_serpro()
    return new_state
