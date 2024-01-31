# -*- coding: utf-8 -*-
"""Module to render jinja templates"""
import re

from jinja2 import Environment


def render_template(
    template_string: str,
    execution_mode: str,
    _vars: dict,
    normalize: bool = False,
):
    def is_incremental() -> bool:
        return execution_mode == "incr"

    template_env = Environment()
    template_env.globals["is_incremental"] = is_incremental
    template = template_env.from_string(template_string)

    rendered_string = template.render(_vars)

    if normalize:
        rendered_string = re.sub(r"\s+", " ", rendered_string).strip()

    return rendered_string
