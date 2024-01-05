# -*- coding: utf-8 -*-
from prefeitura_rio.pipelines_utils.infisical import get_infisical_client

from pipelines.utils.utils import normalize_keys


def get_secret(secret_path: str = "/", secret_name: str = None, environment: str = "dev"):
    """
    Fetches secrets from Infisical. If passing only `secret_path` and
    no `secret_name`, returns all secrets inside a folder.

    Args:
        secret_name (str, optional): _description_. Defaults to None.
        secret_path (str, optional): _description_. Defaults to '/'.
        environment (str, optional): _description_. Defaults to 'dev'.

    Returns:
        _type_: _description_
    """
    client = get_infisical_client()
    if not secret_path.startswith("/"):
        secret_path = f"/{secret_path}"
    if secret_path and not secret_name:
        secrets = client.get_all_secrets(path=secret_path)
        return normalize_keys({s.secret_name: s.secret_value for s in secrets})
    secret = client.get_secret(secret_name=secret_name, path=secret_path, environment=environment)
    return {secret_name: secret.secret_value}
