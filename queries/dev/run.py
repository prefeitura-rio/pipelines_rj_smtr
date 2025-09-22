# -*- coding: utf-8 -*-
# import os
from queries.dev.utils import run_dbt_tests, run_dbt_selector, run_dbt


run_dbt(
    selector_name="snapshot_subsidio",
    resource="snapshot",
    flags = "--target dev --defer --state target-base --favor-state",

)