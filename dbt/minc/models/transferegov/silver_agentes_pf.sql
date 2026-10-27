{{ config(materialized="table") }}
-- Silver transferegov — perfil LPG Pessoa Física: base + variantes audiovisual e
-- multicultural, fatias de bronze_planilha_dados_lpg por tabela_origem.
-- Corta o lixo de parsing (linha sem CPF) e normaliza a chave. `table`, e não
-- view: o corte reduz milhões de linhas da planilha a ~20 mil, e quem lê é a
-- gold eixo2_meta3_perfil_agente, que faz union e dedup em cima.
with
    unioned as (
        select
            "nº do cpf" as documento,
            "raça, cor ou etnia" as raca_bruto,
            "é pessoa com deficiência –pcd?" as pcd_bruto,
            cep,
            cidade,
            uf,
            tabela_origem as origem_tabela,
            nome_programa
        from {{ ref("bronze_planilha_dados_lpg") }}
        where
            tabela_origem in (
                'lpg_dados_pessoa_fisica',
                'lpg_dados_pessoa_fisica_audiovisual',
                'lpg_dados_pessoa_fisica_multicultur'
            )
    )
select
    {{ normaliza_documento("documento") }} as identificador_unico,
    'pf' as tipo_proponente,
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
