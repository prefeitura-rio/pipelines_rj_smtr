# -*- coding: utf-8 -*-
"""Prefect functions"""
import inspect
import json
from typing import Any, Callable, Dict, Type, Union

import prefect
from prefect import unmapped
from prefect.backend.flow_run import FlowRunView, FlowView, watch_flow_run
from prefect.engine.state import Cancelled, State

# from prefect.engine.signals import PrefectStateSignal, signal_from_state
from prefect.tasks.prefect import create_flow_run, wait_for_flow_run
from prefeitura_rio.pipelines_utils.logging import log

from pipelines.constants import constants
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


def get_current_flow_labels() -> list[str]:
    """
    Get the labels of the current flow.
    """
    flow_run_id = prefect.context.get("flow_run_id")
    flow_run_view = FlowRunView.from_flow_run_id(flow_run_id)
    return flow_run_view.labels


def create_subflow_run(
    flow_name: str,
    parameters: dict,
    idempotency_key: str,
    project_name: str = None,
    labels: list[str] = None,
) -> str:
    """
    Executa um subflow

    Args:
        flow_name (str): Nome do flow a ser executado.
        parameters (dict): Parâmetros para executar o flow
        idempotency_key (str): Uma chave única para a run do flow, execuções de flows
            com a mesma idempotency_key são consideradas a mesma
        project_name (str, optional): Nome do projeto no Prefect para executar o flow,
            se não for especificado, é utilizado o nome do projeto do flow atual
        labels (list[str]): Labels para executar o flow,
            se não for especificado, são utilizadas as labels do flow atual

    Returns:
        str: o id da execução do flow
    """

    if prefect.context["flow_name"] == flow_name:
        raise RecursionError("Can not run recursive flows")

    if project_name is None:
        project_name = prefect.context.get("project_name")

    if labels is None:
        labels = get_current_flow_labels()

    log(
        f"""Will run flow with the following data:
        flow name: {flow_name}
        project name: {project_name}
        labels: {labels}
        parameters: {parameters}
    """
    )

    flow = FlowView.from_flow_name(flow_name, project_name=project_name)

    client = prefect.Client()

    flow_run_id = client.create_flow_run(
        flow_id=flow.flow_id,
        parameters=parameters,
        labels=labels,
        idempotency_key=idempotency_key,
    )

    with prefect.context.get("_subflow_ids", []) as subflow_ids:
        subflow_ids.append(flow_run_id)

    run_url = constants.FLOW_RUN_URL_PATTERN.value.format(run_id=flow_run_id)

    log(f"Created flow run: {run_url}")

    return flow_run_id


def wait_subflow_run(flow_run_id: str) -> FlowRunView:
    flow_run = FlowRunView.from_flow_run_id(flow_run_id)

    for exec_log in watch_flow_run(
        flow_run_id,
        stream_states=True,
        stream_logs=True,
    ):
        message = f"Flow {flow_run.name!r}: {exec_log.message}"
        prefect.context.logger.log(exec_log.level, message)

    flow_run = flow_run.get_latest()

    # state_signal = signal_from_state(flow_run.state)(
    #     message=f"{flow_run_id} finished in state {flow_run.state}",
    #     result=flow_run,
    # )
    return flow_run


def run_flow_mapped(
    flow_name: str,
    parameters: list[dict],
    project_name: str = None,
    labels: list[str] = None,
    maximum_parallelism: int = None,
):
    """
    Executa e espera várias execuções de um mesmo flow em paralelo
    com diferentes argumentos

    Args:
        flow_name (str): Nome do flow a ser executado.
        parameters (list[dict]): Lista de parâmetros para cada execução do flow.
        project_name (str, optional): Nome do projeto no Prefect para executar o flow,
            se não for especificado, é utilizado o nome do projeto do flow atual
        labels (list[str]): Labels para executar o flow,
            se não for especificado, são utilizadas as labels do flow atual

    Returns:
        FunctionTask: retorno da task wait_for_flow_run
    """
    if not isinstance(parameters, list):
        raise ValueError("Parameters must be a list")

    if prefect.context["flow_name"] == flow_name:
        raise ValueError("Can not run recursive flows")

    if project_name is None:
        project_name = prefect.context.get("project_name")

    if labels is None:
        labels = get_current_flow_labels()

    if maximum_parallelism is None:
        execution_list = [parameters]
    else:
        execution_list = [
            parameters[i : i + maximum_parallelism]  # noqa
            for i in range(0, len(parameters), maximum_parallelism)
        ]

    complete_wait = []
    for params in execution_list:
        subflow_runs = create_flow_run.map(
            flow_name=unmapped(flow_name),
            project_name=unmapped(project_name),
            labels=unmapped(labels),
            parameters=params,
        )

        wait_runs = wait_for_flow_run.map(
            subflow_runs,
            stream_states=unmapped(True),
            stream_logs=unmapped(True),
            raise_final_state=unmapped(True),
        )
        complete_wait.append(wait_runs)

    return complete_wait


def handler_cancel_subflows(obj, old_state: State, new_state: State) -> State:
    if isinstance(new_state, Cancelled):
        client = prefect.Client()
        subflows = prefect.context.get("_subflow_ids", [])
        if len(subflows) > 0:
            query = f"""
                query {{
                    flow_run(
                        where: {{
                            _and: [
                                {{state: {{_eq: "Running"}}}},
                                {{id: {{_in: {json.dumps(subflows)}}}}}
                            ]
                        }}
                    ) {{
                        id
                    }}
                }}
            """
            # pylint: disable=no-member
            response = client.graphql(query=query)
            active_subflow_runs = response["data"]["flow_run"]
            if active_subflow_runs:
                logger = prefect.context.get("logger")
                logger.info(f"Found {len(active_subflow_runs)} subflows running")
                for subflow_run_id in active_subflow_runs:
                    logger.info(f"cancelling run: {subflow_run_id}")
                    client.cancel_flow_run(flow_run_id=subflow_run_id)
                    logger("Run cancelled!")
    return new_state


class FailedSubFlow(Exception):
    """Erro para ser usado quando um subflow falha"""

    pass
