{{ config(materialized="view") }}

with
    historico as (
        select
            * except (
                datetime_ultima_atualizacao,
                dbt_scd_id,
                dbt_updated_at,
                dbt_valid_from,
                dbt_valid_to
            ),
            safe_cast(
                datetime_ultima_atualizacao as datetime
            ) as datetime_ultima_atualizacao,
            dbt_scd_id as id_alteracao,
            safe_cast(dbt_updated_at as datetime) as datetime_ultima_alteracao,
            safe_cast(dbt_valid_from as datetime) as datetime_inicio_vigencia,
            safe_cast(dbt_valid_to as datetime) as datetime_fim_vigencia
        from {{ ref("snapshot_transito_autuacao") }}
    )
select *
from historico
