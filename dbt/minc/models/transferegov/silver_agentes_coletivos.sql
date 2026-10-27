{{ config(materialized="table") }}
-- Silver transferegov — perfil LPG Coletivos/Grupos: base + variantes, fatias de
-- bronze_planilha_dados_lpg por tabela_origem. Demografia = maioria do grupo.
-- Corta linha sem CPF do representante e normaliza a chave. `table` pelo mesmo
-- motivo de silver_agentes_pf.
with
    unioned as (
        select
            "nº do cpf do representante do grupo/coletivo" as documento,
            "raça, cor ou etnia da maioria do grupo/coletivo" as raca_bruto,
            "há pessoa com deficiência – pcd no grupo/coletivo?" as pcd_bruto,
            cep,
            cidade,
            uf,
            tabela_origem as origem_tabela,
            nome_programa
        from {{ ref("bronze_planilha_dados_lpg") }}
        where
            tabela_origem in (
                'lpg_dados_coletivos',
                'lpg_dados_grupo_coletivo',
                'lpg_dados_grupo_coletivo_audiovisua',
                'lpg_dados_grupo_coletivo_multicultu'
            )
    )
select
    {{ normaliza_documento("documento") }} as identificador_unico,
    'coletivo' as tipo_proponente,
    'lpg' as origem,
    raca_bruto,
    pcd_bruto,
    cast(null as text) as indigena_bruto,
    cast(null as text) as quilombola_bruto,
    cep,
    cidade,
    uf,
    origem_tabela,
    nome_programa
from unioned
where {{ normaliza_documento("documento") }} is not null
