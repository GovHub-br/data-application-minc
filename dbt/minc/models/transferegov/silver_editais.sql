{{ config(materialized="table") }}
-- Silver transferegov — editais (denominador / derivação de ano): a aba de
-- editais da LPG (bronze_planilha_editais_lpg) mais as quatro abas de
-- instrumentos, fatias de bronze_planilha_dados_lpg. Colunas via coalesce_por_nome
-- sobre as tabelas de origem (origem_da_bronze; ver silver_contemplados_lpg).
-- `table`: corta linha sem nome e sem número de edital.
{% set ed = (
    origem_da_bronze("bronze_planilha_editais_lpg")
    if execute
    else ref("bronze_planilha_editais_lpg")
) %}
{% set inst = (
    origem_da_bronze("bronze_planilha_dados_lpg")
    if execute
    else ref("bronze_planilha_dados_lpg")
) %}
with
    editais as (
        select
            {{ coalesce_por_nome(ed, ["nome do edital"]) }} as nome_edital,
            cast(null as text) as numero_edital,
            {{ coalesce_por_nome(ed, ["valor total do edital"]) }} as valor_total_raw,
            nome_arquivo,
            nome_programa
        from {{ ref("bronze_planilha_editais_lpg") }}
        union all
        -- As quatro abas de instrumentos eram quatro tabelas com o mesmo layout;
        -- hoje são um filtro só.
        select
            {{ coalesce_por_nome(inst, ["título do edital"]) }} as nome_edital,
            {{ coalesce_por_nome(inst, ["número do edital"]) }} as numero_edital,
            {{ coalesce_por_nome(inst, ["valor total do edital"]) }} as valor_total_raw,
            nome_arquivo,
            nome_programa
        from {{ ref("bronze_planilha_dados_lpg") }}
        where
            tabela_origem in (
                'lpg_dados_instrumentos',
                'lpg_dados_instrumentos_2',
                'lpg_dados_instrumentos_2_2',
                'lpg_dados_instrumentos_publicos'
            )
    )
select
    nome_edital,
    numero_edital,
    {{ parse_valor("valor_total_raw") }} as valor_total,
    nome_arquivo,
    nome_programa
from editais
where nome_edital is not null or numero_edital is not null
