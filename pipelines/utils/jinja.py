# -*- coding: utf-8 -*-
"""Module to render jinja templates"""
import re

from jinja2 import Environment

# from pipelines.constants import constants


def render_template(
    template_string: str,
    execution_mode: str,
    _vars: dict,
    normalize: bool = False,
) -> str:
    """
    Renderiza um template Jinja

    a macro is_incremental() pode ser usada da mesma forma que no DBT

    Args:
        template_string (str): A string a ser tratada
        execution_mode (str): full ou incr
        _vars (dict): Dicionário no formato {nome_variavel: valor_variavel, ...}
        normalize (bool, optional): Se True, remove quebras de linha, espaços duplos e tabs,
            criando a string final com uma apenas linha. Defaults to False

    Returns:
        str: A string renderizada

    """

    # def is_incremental() -> bool:
    #     return execution_mode == constants.MODE_INCR.value

    template_env = Environment()
    # template_env.globals["is_incremental"] = is_incremental
    template = template_env.from_string(template_string)

    rendered_string = template.render(_vars)

    if normalize:
        rendered_string = re.sub(r"\s+", " ", rendered_string).strip()

    return rendered_string
