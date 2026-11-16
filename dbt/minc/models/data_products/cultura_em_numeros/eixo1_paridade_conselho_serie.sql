-- Gold do Eixo 1: a composição dos conselhos municipais de cultura de 2014 a
-- 2026, a Figura 15 da publicação inteira, que no repositório do Eixo 1 é
-- mantida à mão em outputs/. Os anos da MUNIC (2014, 2018 e 2021) vêm de
-- eixo1_munic_paridade_conselho; o ponto do SNC vem da linha 'Conselho
-- paritário?' de eixo1_snc_perfil_conselho, com 'Sim' como 'Paritário' e 'Não'
-- como 'Não Paritário', no ano da versão da lista de entes do portal.
--
-- O UNIVERSO MUDA ENTRE AS FONTES. Na MUNIC é o município que respondeu ao
-- IBGE que tem conselho de cultura e informou a composição: pesquisa,
-- autodeclarada. No SNC é o município com a lei do conselho 'Concluída' na
-- Plataforma, isto é, com o documento enviado e validado: registro
-- administrativo. Os quatro pontos não medem a mesma população.
--
-- Conta só 'Paritário' e 'Não Paritário', e a proporção é dentro do ano e da
-- fonte, como no R.
with

    munic as (
        select
            ano as ano_pesquisa,
            'MUNIC' as fonte,
            composicao_conselho,
            quantidade_municipios
        from {{ ref("eixo1_munic_paridade_conselho") }}
    ),

    versao_snc as (
        select max(versao) as versao from {{ ref("eixo1_snc_painel_municipio") }}
    ),

    snc as (
        select
            p.quantidade_municipios,
            'SNC' as fonte,
            cast(extract(year from v.versao) as int) as ano_pesquisa,
            case
                p.resposta when 'Sim' then 'Paritário' when 'Não' then 'Não Paritário'
            end as composicao_conselho
        from {{ ref("eixo1_snc_perfil_conselho") }} as p
        cross join versao_snc as v
        where p.caracteristica = 'Conselho paritário?' and p.resposta in ('Sim', 'Não')
    ),

    serie as (
        select ano_pesquisa, fonte, composicao_conselho, quantidade_municipios
        from munic
        union all
        select ano_pesquisa, fonte, composicao_conselho, quantidade_municipios
        from snc
    )

select
    ano_pesquisa,
    fonte,
    composicao_conselho,
    quantidade_municipios,
    cast(
        sum(quantidade_municipios) over (partition by ano_pesquisa, fonte) as bigint
    ) as total_municipios,
    cast(quantidade_municipios as numeric)
    / sum(quantidade_municipios) over (partition by ano_pesquisa, fonte) as proporcao
from serie
