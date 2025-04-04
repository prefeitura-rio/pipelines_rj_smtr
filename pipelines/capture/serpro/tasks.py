# -*- coding: utf-8 -*-
from datetime import datetime, timedelta

from prefect import task

from pipelines.constants import constants as smtr_constants
from pipelines.serpro.constants import constants
from pipelines.utils.gcp.bigquery import SourceTable
from pipelines.utils.jdbc import JDBC
from pipelines.utils.utils import log


@task(
    max_retries=smtr_constants.MAX_RETRIES.value,
    retry_delay=timedelta(seconds=smtr_constants.RETRY_DELAY.value),
)
def create_serpro_extractor(
    source: SourceTable,  # pylint: disable=W0613
    timestamp: datetime,
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
        start_date = timestamp.date().strftime("%Y-%m-%d")

        if timestamp.month == 12:
            next_month = timestamp.replace(year=timestamp.year + 1, month=1, day=1)
        else:
            next_month = timestamp.replace(month=timestamp.month + 1, day=1)

        last_day = next_month - timedelta(days=1)
        end_date = last_day.date().strftime("%Y-%m-%d")

        jdbc = JDBC(db_params_secret_path="radar_serpro", environment="dev")

        query = constants.SERPRO_CAPTURE_PARAMS.value["query"].format(
            start_date=start_date, end_date=end_date
        )

        jdbc.execute_query(query)
        columns = jdbc.get_columns()

        result = []
        batch_size = 100000

        while True:
            rows = jdbc.fetch_batch(batch_size=batch_size)
            if not rows:
                break
            for row in rows:
                result.append(dict(zip(columns, row)))

        log(f"Extracted {len(result)} records from SERPRO")

        return result

    return extract_data
