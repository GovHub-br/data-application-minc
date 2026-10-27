{{ config(materialized='table') }}
-- Silver transferegov — perfil LPG Pessoa Jurídica: base + variantes, fatias de
-- bronze_planilha_dados_lpg por tabela_origem. Demografia = corpo diretivo.
-- Corta linha sem CNPJ e normaliza a chave. `table` pelo mesmo motivo de
-- silver_agentes_pf.
with unioned as (
    select
        "nº do cnpj"                                               as documento,
        "raça, cor ou etnia da maioria do corpo diretivo da pj"    as raca_bruto,
        "há pessoa com deficiência – pcd no corpo diretivo da pj?" as pcd_bruto,
        cep,
        cidade,
        uf,
        tabela_origem                                              as origem_tabela,
        nome_programa
    from {{ ref('bronze_planilha_dados_lpg') }}
    where tabela_origem in (
        'lpg_dados_pessoa_juridica',
        'lpg_dados_pessoa_juridica_audiovisu',
        'lpg_dados_pessoa_juridica_multicult'
    )
)
select
    {{ normaliza_documento('documento') }} as identificador_unico,
    'pj'                                    as tipo_proponente,
    'lpg'                                   as origem,
    raca_bruto,
    pcd_bruto,
    cast(null as text)                      as indigena_bruto,
    cast(null as text)                      as quilombola_bruto,
    cep,
    cidade,
    uf,
    origem_tabela,
    nome_programa
from unioned
where {{ normaliza_documento('documento') }} is not null
