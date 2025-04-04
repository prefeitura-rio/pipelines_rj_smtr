# -*- coding: utf-8 -*-
import csv
import io
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
        ts = datetime.fromisoformat(timestamp) if isinstance(timestamp, str) else timestamp

        start_date = ts.date().strftime("%Y-%m-%d")

        if ts.month == 12:
            next_month = ts.replace(year=ts.year + 1, month=1, day=1)
        else:
            next_month = ts.replace(month=ts.month + 1, day=1)

        last_day = next_month - timedelta(days=1)
        end_date = last_day.date().strftime("%Y-%m-%d")

        jdbc = JDBC(db_params_secret_path="radar_serpro", environment="dev")

        query = constants.SERPRO_CAPTURE_PARAMS.value["query"].format(
            start_date=start_date, end_date=end_date
        )

        try:
            jdbc.execute_query(query)
            columns = jdbc.get_columns()
        except Exception as e:
            log(f"Erro ao executar query ou obter colunas: {str(e)}")
            raise
        output = io.StringIO()
        csv_writer = csv.writer(output)

        csv_writer.writerow(columns)

        batch_size = 100000
        total_rows = 0

        while True:
            rows = jdbc.fetch_batch(batch_size=batch_size)
            if not rows:
                break

            for row in rows:
                csv_writer.writerow(row)
                total_rows += 1

        log(f"Total de registros encontrados: {total_rows}")

        return output.getvalue()

    return extract_data
