{% test subsidio_viagens_atualizadas(model) -%}
WITH
                viagem_completa AS (
                SELECT
                    data,
                    datetime_ultima_atualizacao
                FROM
                    -- rj-smtr.projeto_subsidio_sppo.viagem_completa
                    {{ ref('viagem_completa') }}
                WHERE
                    DATA >= "2024-04-01"
                    AND DATA BETWEEN DATE("{{ var('start_timestamp') }}")
                    AND DATE("{{ var('end_timestamp') }}")),
                sumario_servico_dia_historico AS (
                SELECT
                    data,
                    datetime_ultima_atualizacao
                FROM
                    {{ model }}
                WHERE
                    DATA BETWEEN DATE("{{ var('start_timestamp') }}")
                    AND DATE("{{ var('end_timestamp') }}"))
                SELECT
                DISTINCT DATA
                FROM
                viagem_completa as c
                LEFT JOIN
                sumario_servico_dia_historico AS h
                USING
                (DATA)
                WHERE
                c.datetime_ultima_atualizacao > h.datetime_ultima_atualizacao
{%- endtest %}