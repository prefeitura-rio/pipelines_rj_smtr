# -*- coding: utf-8 -*-
"""Prefect functions"""
import inspect
from typing import Any, Callable, Dict, Type, Union

import prefect

from pipelines.utils.capture.base import DataExtractor


class TypedParameter(prefect.Parameter):
    """
    Parâmetro do Prefect com verificação de tipos

    Args:
        accepted_types Union[tuple[Type], Type]: Tipo ou tupla de tipos aceitos pelo parâmetro
        **parameter_kwargs: Parâmetros para ser passados à classe Parametro padrão do Prefect
    """

    def __init__(self, accepted_types: Union[tuple[Type], Type], **parameter_kwargs):
        self.accepted_types = accepted_types
        super().__init__(**parameter_kwargs)

    def run(self) -> Any:
        """
        Metodo padrão do parâmetro do Prefect, mas com teste de tipagem
        """
        param_value = super().run()
        assert isinstance(
            param_value, self.accepted_types
        ), f"Param {self.name} must be {self.accepted_types}. Received {type(param_value)}"

        return param_value


def extractor_task(func: Callable, **task_init_kwargs):
    """
    Decorator para tasks create_extractor_task do flow generico de captura
    Usado da mesma forma que o decorator task padrão do Prefect.

    Garante que os argumentos e retorno da task estão corretos e
    possibilita que a task seja criada sem precisar de todos os argumentos passados pelo flow
    """
    task_init_kwargs["name"] = task_init_kwargs.get("name", func.__name__)
    signature = inspect.signature(func)
    assert task_init_kwargs.get("nout", 1) == 1, "nout must be 1"
    assert issubclass(
        signature.return_annotation,
        DataExtractor,
    ), "return must be DataExtractor subclass"

    def decorator(func):
        expected_arguments = [
            "env",
            "dataset_id",
            "table_id",
            "save_filepath",
            "data_extractor_params",
            "incremental_info",
        ]

        function_arguments = [p.name for p in signature.parameters.values()]

        invalid_args = [a for a in function_arguments if a not in expected_arguments]

        if len(invalid_args) > 0:
            raise ValueError(f"Invalid arguments: {', '.join(invalid_args)}")

        def wrapper(**kwargs):
            return func(**{k: v for k, v in kwargs.items() if k in function_arguments})

        task_init_kwargs["checkpoint"] = False
        return prefect.task(wrapper, **task_init_kwargs)

    if func is None:
        return decorator
    return decorator(func=func)


def run_local(flow: prefect.Flow, parameters: Dict[str, Any] = None):
    """
    Executa um flow localmente
    """
    # Setup for local run
    flow.storage = None
    flow.run_config = None
    flow.schedule = None
    flow.state_handlers = []

    # Run flow
    return flow.run(parameters=parameters) if parameters else flow.run()


def flow_is_running_local() -> bool:
    """
    Testa se o flow está rodando localmente

    Returns:
        bool: True se está rodando local, False se está na nuvem
    """
    return prefect.context.get("project_name") is None


def rename_current_flow_run(name: str) -> bool:
    """
    Renomeia a run atual do Flow

    Returns:
        bool: Se o flow foi renomeado
    """
    if not flow_is_running_local():
        flow_run_id = prefect.context.get("flow_run_id")
        client = prefect.Client()
        return client.set_flow_run_name(flow_run_id, name)
    return False
