# -*- coding: utf-8 -*-
import subprocess
from datetime import datetime, timedelta
from typing import List

from prefect.engine.state import State
from prefect.schedules import Schedule
from prefect.schedules.clocks import DatesClock

from pipelines.utils.secret import get_secret
from pipelines.utils.utils import log


def setup_serpro(secret_path: str = "radar_serpro"):
    data = get_secret(secret_path=secret_path)["setup.sh"]

    subprocess.run(["touch", "setup.sh"])
    with open("setup.sh", "w") as f:
        f.write(data)

    result = subprocess.run(["sh", "setup.sh"], capture_output=True, text=True)

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


def create_monthly_dates(start_date: datetime, end_date: datetime) -> List[datetime]:
    """
    Cria uma lista de datas no primeiro dia de cada mês, do start_date até o end_date.

    Args:
        start_date: Data inicial (será ajustada para o primeiro dia do mês)
        end_date: Data final (será ajustada para o primeiro dia do mês)

    Returns:
        List[datetime]: Lista com o primeiro dia de cada mês no intervalo
    """
    current = datetime(start_date.year, start_date.month, 1)
    dates = []

    while current <= end_date:
        dates.append(current)
        if current.month == 12:
            current = datetime(current.year + 1, 1, 1)
        else:
            current = datetime(current.year, current.month + 1, 1)

    return dates


def create_serpro_schedule() -> Schedule:
    """
    Cria um schedule sequencial que executa uma única vez para cada mês,
    com execuções consecutivas espaçadas por 30 minutos.

    Returns:
        Schedule: O schedule configurado para o flow
    """
    start_date = datetime(2023, 5, 1)
    end_date = datetime(2025, 3, 31)

    monthly_timestamps = create_monthly_dates(start_date, end_date)

    execution_dates = []
    base_time = datetime.now() + timedelta(minutes=5)

    for i, timestamp in enumerate(monthly_timestamps):
        execution_date = base_time + timedelta(minutes=30 * i)
        execution_dates.append(execution_date)

        log(f"Mês: {timestamp.strftime('%Y-%m')} será executado em: {execution_date}")

    clocks = []
    for exec_date, timestamp in zip(execution_dates, monthly_timestamps):
        timestamp_str = timestamp.isoformat()
        clock = DatesClock(dates=[exec_date], parameter_defaults={"timestamp": timestamp_str})
        clocks.append(clock)

    return Schedule(clocks=clocks)
