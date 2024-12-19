# -*- coding: utf-8 -*-
from prefeitura_rio.pipelines_utils.logging import log
from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError

ENGINE_MAPPING = {
    "mysql": {"driver": "pymysql", "port": "3306"},
    "postgresql": {"driver": "psycopg2", "port": "5432"},
}


def create_database_url(
    engine: str,
    host: str,
    user: str,
    password: str,
    database: str,
) -> str:
    """
    Cria a URL para se conectar a um banco de dados

    Args:
        engine (str): O banco de dados (postgresql ou mysql)
        host (str): O host do banco de dados
        user (str): O usuário para se conectar
        password (str): A senha do usuário
        database (str): O nome da base (schema)

    Returns:
        str: a URL de conexão
    """
    engine_info = ENGINE_MAPPING[engine]
    driver = engine_info["driver"]
    port = engine_info["port"]
    return f"{engine}+{driver}://{user}:{password}@{host}:{port}/{database}"


def test_database_connection(
    engine: str,
    host: str,
    user: str,
    password: str,
    database: str,
) -> tuple[bool, str]:
    """
    Testa se é possível se conectar a um banco de dados

    Args:
        engine (str): O banco de dados (postgresql ou mysql)
        host (str): O host do banco de dados
        user (str): O usuário para se conectar
        password (str): A senha do usuário
        database (str): O nome da base (schema)

    Returns:
        bool: Se foi possível se conectar ou não
        str: String do erro
    """
    url = create_database_url(
        engine=engine,
        host=host,
        user=user,
        password=password,
        database=database,
    )
    connection = create_engine(url)
    log(f"Tentando conexão com o banco de dados {database}")
    try:
        with connection.connect() as _:
            log("Conexão bem-sucedida!")
            return True, None
    except OperationalError as e:
        log("Conexão falhou", level="warning")
        return False, str(e)
