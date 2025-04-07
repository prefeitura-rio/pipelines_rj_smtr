# -*- coding: utf-8 -*-
import csv
import os
import tempfile
from datetime import datetime, timedelta
from typing import Union

from prefect import task

from pipelines.capture.serpro.constants import constants
from pipelines.constants import constants as smtr_constants
from pipelines.utils.gcp.bigquery import SourceTable
from pipelines.utils.jdbc import JDBC
from pipelines.utils.utils import log


@task(
    max_retries=smtr_constants.MAX_RETRIES.value,
    retry_delay=timedelta(seconds=smtr_constants.RETRY_DELAY.value),
)
def create_serpro_extractor(
    source: SourceTable, timestamp: Union[str, datetime]  # pylint: disable=W0613
):
    """
    Cria uma função para extrair dados do SERPRO

    Args:
        source (SourceTable): Objeto contendo informações da tabela
        timestamp (datetime): Timestamp da execução

    Returns:
        Callable: Função para extração dos dados
    """

    def extract_data():
        """
        Extrai dados do SERPRO
        """
        ts = datetime.fromisoformat(timestamp) if isinstance(timestamp, str) else timestamp
        start_date = ts.date()

        if ts.month == 12:
            next_month = ts.replace(year=ts.year + 1, month=1, day=1)
        else:
            next_month = ts.replace(month=ts.month + 1, day=1)

        last_day = next_month - timedelta(days=1)
        end_date = last_day.date()

        with tempfile.NamedTemporaryFile(mode="w+", delete=False, suffix=".csv") as temp_file:
            csv_writer = csv.writer(temp_file)
            header = False
            total_rows = 0

            current_date = start_date
            partition_size = timedelta(days=5)

            try:
                jdbc = JDBC(db_params_secret_path="radar_serpro", environment="dev")

                while current_date <= end_date:
                    partition_end = min(current_date + partition_size, end_date)

                    log(f"Processando partição: {current_date} até {partition_end}")

                    query = constants.SERPRO_CAPTURE_PARAMS.value["query"].format(
                        start_date=current_date.strftime("%Y-%m-%d"),
                        end_date=partition_end.strftime("%Y-%m-%d"),
                    )

                    jdbc.execute_query(query)

                    if not header:
                        columns = jdbc.get_columns()
                        csv_writer.writerow(columns)
                        header = True

                    batch_size = 5000
                    while True:
                        rows = jdbc.fetch_batch(batch_size=batch_size)
                        if not rows:
                            break
                        csv_writer.writerows(rows)
                        total_rows += len(rows)

                    current_date = partition_end + timedelta(days=1)

                log(f"Total de registros encontrados: {total_rows}")

                temp_file_name = temp_file.name

            except Exception as e:
                log(f"Erro ao extrair dados do SERPRO: {str(e)}", level="error")
                raise
            finally:
                jdbc.close()

        with open(temp_file_name, "r") as f:
            result = f.read()

        os.unlink(temp_file_name)

        return result

    return extract_data
