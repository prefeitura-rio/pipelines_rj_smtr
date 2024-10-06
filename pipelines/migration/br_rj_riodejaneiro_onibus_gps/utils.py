# -*- coding: utf-8 -*-
import prefect
from prefect.engine.state import State

from pipelines.constants import constants as smtr_constants
from pipelines.migration.tasks import format_send_discord_message
from pipelines.utils.secret import get_secret

prefect.context


def handler_notify_fail(obj, old_state: State, new_state: State) -> State:
    """Notifica no Discord caso o flow falhe"""

    if new_state.is_failed():
        webhook_url = get_secret(secret_path=smtr_constants.WEBHOOKS_SECRET_PATH.value)["gps_sppo"]

        dados_tag = f"<@&{smtr_constants.OWNERS_DISCORD_MENTIONS.value['dados_smtr']['user_id']}>"
        prefect_url = "https://pipelines.dados.rio/smtr"
        format_send_discord_message(
            formatted_messages=[
                f"{dados_tag} :warning: Falha no flow {prefect.context['flow_name']}!\n",
                f"**link:** {prefect_url}/flow-run/{prefect.context['flow_run_id']}",
            ],
            webhook_url=webhook_url,
        )
