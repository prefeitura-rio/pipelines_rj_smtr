# -*- coding: utf-8 -*-
import csv
import os
import subprocess
import tempfile
from datetime import datetime, timedelta
from typing import List

from prefect.engine.state import State
from prefect.schedules import Schedule
from prefect.schedules.clocks import DatesClock
from pytz import timezone

from pipelines.capture.serpro.constants import constants as serpro_constants
from pipelines.constants import constants
from pipelines.utils.jdbc import JDBC
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
    base_time = datetime.now(tz=timezone(constants.TIMEZONE.value)) + timedelta(minutes=5)

    for i, timestamp in enumerate(monthly_timestamps):
        execution_date = base_time + timedelta(minutes=30 * i)
        execution_dates.append(execution_date)

    clocks = []
    for exec_date, timestamp in zip(execution_dates, monthly_timestamps):
        timestamp_str = timestamp.isoformat()
        clock = DatesClock(dates=[exec_date], parameter_defaults={"timestamp": timestamp_str})
        clocks.append(clock)

    return Schedule(clocks=clocks)


def extract_serpro_data(timestamp):
    """
    Extrai dados do SERPRO

    Args:
        timestamp (datetime ou str): Timestamp da execução

    Returns:
        str: Dados extraídos em formato CSV
    """
    ts = datetime.fromisoformat(timestamp) if isinstance(timestamp, str) else timestamp

    update_date = ts.date().strftime("%Y-%m-%d")

    try:
        jdbc = JDBC(db_params_secret_path="radar_serpro", environment="dev")

        query = serpro_constants.SERPRO_CAPTURE_PARAMS.value["query"].format(
            update_date=update_date
        )

        jdbc.execute_query(query)
        columns = jdbc.get_columns()

        with tempfile.NamedTemporaryFile(mode="w+", delete=False, suffix=".csv") as temp_file:
            csv_writer = csv.writer(temp_file)

            csv_writer.writerow(columns)

            batch_size = 50000
            total_rows = 0

            while True:
                rows = jdbc.fetch_batch(batch_size=batch_size)
                if not rows:
                    break

                csv_writer.writerows(rows)
                total_rows += len(rows)

            temp_file_name = temp_file.name

            log(f"Total de registros encontrados: {total_rows}")

            with open(temp_file_name, "r") as f:
                result = f.read()

            os.unlink(temp_file_name)
            return result
    except Exception as e:
        log(f"Erro ao extrair dados do SERPRO: {str(e)}", level="error")
        raise
    finally:
        jdbc.close()
