# -*- coding: utf-8 -*-
from prefeitura_rio.pipelines_utils.infisical import get_infisical_client


def get_secret(secret_path: str = "/", secret_name: str = None, environment: str = "dev"):
    """
    Pega os dados de um secret no Infisical. Se for passado somente um secret_path
    sem o argumento secret_name, retorna todos os secrets dentro da pasta.

    Args:
        secret_path (str, optional): Pasta do secret no infisical. Defaults to "/".
        secret_name (str, optional): Nome do secret. Defaults to None.
        environment (str, optional): Ambiente para ler o secret. Defaults to 'dev'.

    Returns:
        dict: Dicionário com os dados retornados do Infisical
    """
    client = get_infisical_client()
    if not secret_path.startswith("/"):
        secret_path = f"/{secret_path}"
    if secret_path and not secret_name:
        secrets = client.get_all_secrets(path=secret_path, environment=environment)
        return {s.secret_name.lower(): s.secret_value for s in secrets}
    secret = client.get_secret(secret_name=secret_name, path=secret_path, environment=environment)
    return {secret_name.lower(): secret.secret_value}
