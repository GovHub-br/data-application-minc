-- Silver ibge — estadic_2023_primeira_infancia.
-- Origem: bronze_estadic_2023_primeira_infancia, cópia fiel de
-- seeds.ibge_estadic_2023_primeira_infancia, onde tudo chega
-- como text da carga de fonte estática (ibge_perfil_dag, ADR 0012).
-- Tipar é o trabalho desta camada.
-- 44 colunas: 2 tipadas, 42 mantidas como texto.
-- O cast vem do padrão medido no dado, não do nome da coluna: exige 100%
-- dos valores preenchidos casando. Resposta do questionário continua texto,
-- como o IBGE publica ('Sim', 'Não', '-', 'Recusa').
select
    {{ bronze_inteiro("cod_uf", "bigint") }} as cod_uf,
    {{ bronze_texto("sigla_uf") }} as sigla_uf,
    {{ bronze_texto("nome_uf") }} as nome_uf,
    {{ bronze_inteiro("popuf", "bigint") }} as popuf,
    {{ bronze_texto("regiao") }} as regiao,
    {{ bronze_texto("epri01") }} as epri01,
    {{ bronze_texto("epri011") }} as epri011,
    {{ bronze_texto("epri012a") }} as epri012a,
    {{ bronze_texto("epri012b") }} as epri012b,
    {{ bronze_texto("epri013") }} as epri013,
    {{ bronze_texto("epri014") }} as epri014,
    {{ bronze_texto("epri02") }} as epri02,
    {{ bronze_texto("epri031") }} as epri031,
    {{ bronze_texto("epri032") }} as epri032,
    {{ bronze_texto("epri033") }} as epri033,
    {{ bronze_texto("epri034") }} as epri034,
    {{ bronze_texto("epri035") }} as epri035,
    {{ bronze_texto("epri036") }} as epri036,
    {{ bronze_texto("epri037") }} as epri037,
    {{ bronze_texto("epri038") }} as epri038,
    {{ bronze_texto("epri039") }} as epri039,
    {{ bronze_texto("epri0310") }} as epri0310,
    {{ bronze_texto("epri04") }} as epri04,
    {{ bronze_texto("epri05") }} as epri05,
    {{ bronze_texto("epri061") }} as epri061,
    {{ bronze_texto("epri062") }} as epri062,
    {{ bronze_texto("epri063") }} as epri063,
    {{ bronze_texto("epri064") }} as epri064,
    {{ bronze_texto("epri065") }} as epri065,
    {{ bronze_texto("epri066") }} as epri066,
    {{ bronze_texto("epri067") }} as epri067,
    {{ bronze_texto("epri068") }} as epri068,
    {{ bronze_texto("epri069") }} as epri069,
    {{ bronze_texto("epri0610") }} as epri0610,
    {{ bronze_texto("epri0611") }} as epri0611,
    {{ bronze_texto("epri0612") }} as epri0612,
    {{ bronze_texto("epri0613") }} as epri0613,
    {{ bronze_texto("epri0614") }} as epri0614,
    {{ bronze_texto("epri0615") }} as epri0615,
    {{ bronze_texto("epri07") }} as epri07,
    {{ bronze_texto("epri071a") }} as epri071a,
    {{ bronze_texto("epri071b") }} as epri071b,
    {{ bronze_texto("edicao") }} as edicao,
    {{ bronze_texto("dt_ingest") }} as dt_ingest
from {{ ref("bronze_estadic_2023_primeira_infancia") }}
