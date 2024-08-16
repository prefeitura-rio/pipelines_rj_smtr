# -*- coding: utf-8 -*-
"""
Modulo com schedules para os Flows da rj-smtr
"""

from datetime import datetime, timedelta

from prefect.schedules import Schedule
from prefect.schedules.clocks import CronClock, IntervalClock
from pytz import timezone

from pipelines.constants import constants
from pipelines.constants import constants as emd_constants


def generate_interval_schedule(
    interval: timedelta, agent_label: str, params: dict = None
) -> Schedule:
    """
    Cria um Schedule para os flows do prefect

    Args:
        interval (timedelta): Frequência do agendamento do flow
        agent_label (str): Label para executar o flow
        params (dict, optional): Parâmetros para ser passados ao flow no
            momento da execução
    """
    if not params:
        params = {}
    return Schedule(
        [
            IntervalClock(
                interval=interval,
                start_date=datetime(2021, 1, 1, 0, 0, 0, tzinfo=timezone(constants.TIMEZONE.value)),
                labels=[
                    agent_label,
                ],
                parameter_defaults=params,
            )
        ]
    )


every_minute = Schedule(
    clocks=[
        IntervalClock(
            interval=timedelta(minutes=1),
            start_date=datetime(2021, 1, 1, 0, 0, 0, tzinfo=timezone(constants.TIMEZONE.value)),
            labels=[
                emd_constants.RJ_SMTR_AGENT_LABEL.value,
            ],
        ),
    ]
)
every_minute_dev = Schedule(
    clocks=[
        IntervalClock(
            interval=timedelta(minutes=1),
            start_date=datetime(2021, 1, 1, 0, 0, 0, tzinfo=timezone(constants.TIMEZONE.value)),
            labels=[
                emd_constants.RJ_SMTR_DEV_AGENT_LABEL.value,
            ],
        ),
    ]
)

every_10_minutes = Schedule(
    clocks=[
        IntervalClock(
            interval=timedelta(minutes=10),
            start_date=datetime(2021, 1, 1, 0, 0, 0, tzinfo=timezone(constants.TIMEZONE.value)),
            labels=[
                emd_constants.RJ_SMTR_AGENT_LABEL.value,
            ],
        ),
    ]
)


every_hour = Schedule(
    clocks=[
        IntervalClock(
            interval=timedelta(hours=1),
            start_date=datetime(2021, 1, 1, 0, 0, 0, tzinfo=timezone(constants.TIMEZONE.value)),
            labels=[
                emd_constants.RJ_SMTR_AGENT_LABEL.value,
            ],
        ),
    ]
)

every_hour_minute_six = Schedule(
    clocks=[
        IntervalClock(
            interval=timedelta(hours=1),
            start_date=datetime(2021, 1, 1, 0, 6, 0, tzinfo=timezone(constants.TIMEZONE.value)),
            labels=[
                emd_constants.RJ_SMTR_AGENT_LABEL.value,
            ],
        ),
    ]
)

every_hour_minute_thirty = Schedule(
    clocks=[
        IntervalClock(
            interval=timedelta(hours=1),
            start_date=datetime(2021, 1, 1, 0, 30, 0, tzinfo=timezone(constants.TIMEZONE.value)),
            labels=[
                emd_constants.RJ_SMTR_AGENT_LABEL.value,
            ],
        ),
    ]
)

every_day = Schedule(
    clocks=[
        IntervalClock(
            interval=timedelta(days=1),
            start_date=datetime(2021, 1, 1, 0, 0, tzinfo=timezone(constants.TIMEZONE.value)),
            labels=[
                emd_constants.RJ_SMTR_AGENT_LABEL.value,
            ],
        ),
    ]
)


every_day_hour_five = Schedule(
    clocks=[
        IntervalClock(
            interval=timedelta(days=1),
            start_date=datetime(2022, 11, 30, 5, 0, tzinfo=timezone(constants.TIMEZONE.value)),
            labels=[
                emd_constants.RJ_SMTR_AGENT_LABEL.value,
            ],
        ),
    ]
)

every_5_minutes = Schedule(
    clocks=[
        IntervalClock(
            interval=timedelta(minutes=5),
            start_date=datetime(2021, 1, 1, 0, 0, 0, tzinfo=timezone(constants.TIMEZONE.value)),
            labels=[
                emd_constants.RJ_SMTR_AGENT_LABEL.value,
            ],
        ),
    ]
)

every_day_hour_seven = Schedule(
    clocks=[
        IntervalClock(
            interval=timedelta(days=1),
            start_date=datetime(2022, 11, 30, 7, 0, tzinfo=timezone(constants.TIMEZONE.value)),
            labels=[
                emd_constants.RJ_SMTR_AGENT_LABEL.value,
            ],
        ),
    ]
)

every_day_hour_seven_minute_five = Schedule(
    clocks=[
        IntervalClock(
            interval=timedelta(days=1),
            start_date=datetime(2022, 11, 30, 7, 5, tzinfo=timezone(constants.TIMEZONE.value)),
            labels=[
                emd_constants.RJ_SMTR_AGENT_LABEL.value,
            ],
        ),
    ]
)

every_dayofmonth_one_and_sixteen = Schedule(
    clocks=[
        CronClock(
            cron="0 12 16 * *",
            start_date=datetime(2022, 12, 16, 12, 0, tzinfo=timezone(constants.TIMEZONE.value)),
            labels=[
                emd_constants.RJ_SMTR_AGENT_LABEL.value,
            ],
        ),
        CronClock(
            cron="0 12 1 * *",
            start_date=datetime(2023, 1, 1, 12, 0, tzinfo=timezone(constants.TIMEZONE.value)),
            labels=[
                emd_constants.RJ_SMTR_AGENT_LABEL.value,
            ],
        ),
    ]
)

every_friday_seven_thirty = Schedule(
    clocks=[
        CronClock(
            cron="30 19 * * 5",
            start_date=datetime(2024, 5, 24, 19, 30, tzinfo=timezone(constants.TIMEZONE.value)),
            labels=[
                emd_constants.RJ_SMTR_AGENT_LABEL.value,
            ],
        )
    ]
)

every_day_hour_ten = Schedule(
    clocks=[
        IntervalClock(
            interval=timedelta(days=1),
            start_date=datetime(2024, 8, 1, 10, 0, tzinfo=timezone(constants.TIMEZONE.value)),
            labels=[
                emd_constants.RJ_SMTR_AGENT_LABEL.value,
            ],
        ),
    ]
)

every_day_hour_ten_minute_five = Schedule(
    clocks=[
        IntervalClock(
            interval=timedelta(days=1),
            start_date=datetime(2024, 8, 1, 10, 5, tzinfo=timezone(constants.TIMEZONE.value)),
            labels=[
                emd_constants.RJ_SMTR_AGENT_LABEL.value,
            ],
        ),
    ]
)

every_day_hour_ten_thirty = Schedule(
    clocks=[
        IntervalClock(
            interval=timedelta(days=1),
            start_date=datetime(2024, 8, 1, 10, 30, tzinfo=timezone(constants.TIMEZONE.value)),
            labels=[
                emd_constants.RJ_SMTR_AGENT_LABEL.value,
            ],
        ),
    ]
)

every_day_hour_eleven = Schedule(
    clocks=[
        IntervalClock(
            interval=timedelta(days=1),
            start_date=datetime(2024, 8, 1, 11, 0, tzinfo=timezone(constants.TIMEZONE.value)),
            labels=[
                emd_constants.RJ_SMTR_AGENT_LABEL.value,
            ],
        ),
    ]
)
