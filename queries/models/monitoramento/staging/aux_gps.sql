{{ config(alias=this.name ~ var("fonte_gps")) }}

select
    data,
    safe_cast(
        datetime(timestamp(datahora), "America/Sao_Paulo") as datetime
    ) datetime_gps,
    safe_cast(ordem as string) id_veiculo,
    concat(
        ifnull(regexp_extract(linha, r'[A-Z]+'), ""),
        ifnull(regexp_extract(linha, r'[0-9]+'), "")
    ) as servico,
    safe_cast(replace(latitude, ',', '.') as float64) latitude,
    safe_cast(replace(longitude, ',', '.') as float64) longitude,
    safe_cast(
        datetime(timestamp(timestamp_captura), "America/Sao_Paulo") as datetime
    ) datetime_captura,
    safe_cast(velocidade as int64) velocidade
from {{ var("sppo_registros_staging") }}
