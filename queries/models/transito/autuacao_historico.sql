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
                datetime(datetime_ultima_atualizacao, "America/Sao_Paulo") as datetime
            ) as datetime_ultima_atualizacao,
            safe_cast(
                datetime(dbt_scd_id, "America/Sao_Paulo") as datetime
            ) as id_alteracao,
            safe_cast(
                datetime(dbt_valid_from, "America/Sao_Paulo") as datetime
            ) as datetime_inicio_vigencia,
            safe_cast(
                datetime(dbt_valid_to, "America/Sao_Paulo") as datetime
            ) as datetime_fim_vigencia
        from {{ ref("snapshot_transito_autuacao") }}
    )
select *
from historico
