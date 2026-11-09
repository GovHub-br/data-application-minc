-- Gold-base do Eixo 1, frente MUNIC: o perfil dos conselhos municipais de
-- cultura, uma linha por município e edição (2014, 2018, 2021), só para os
-- municípios que responderam que têm conselho.
--
-- Reproduz o df_conselhos_detalhado do MUNIC.R. A regra de negócio está aqui:
-- quem conta como tendo conselho, como a resposta sobre a composição vira
-- 'Paritário' ou 'Não Paritário', e como as competências viram uma categoria.
-- As golds de paridade e de competência leem daqui.
--
-- `~*` e não lower(): o lower() do Postgres depende do locale para letra
-- acentuada. Onde o R procura "é paritário" (resposta "É paritário"), o padrão
-- traz as duas formas da letra.
with

    conselhos_2021_2018 as (
        select
            codmun::text as cod_municipio,
            2021 as ano,
            mcul192 as composicao,
            mcul201 as consultivo,
            mcul202 as deliberativo,
            mcul203 as normativo,
            mcul204 as fiscalizador
        from {{ ref("silver_munic_2021_cultura") }}
        where {{ eixo1_sim_nao("mcul19") }} = 'Sim'
        union all
        select
            codmun::text as cod_municipio,
            2018 as ano,
            mcul192 as composicao,
            mcul201 as consultivo,
            mcul202 as deliberativo,
            mcul203 as normativo,
            mcul204 as fiscalizador
        from {{ ref("silver_munic_2018_cultura") }}
        where {{ eixo1_sim_nao("mcul19") }} = 'Sim'
    ),

    classificado_2021_2018 as (
        select
            cod_municipio,
            ano,
            case
                when composicao ~* 'paritá'
                then 'Paritário'
                when composicao ~* 'representação|governamental|sociedade civil'
                then 'Não Paritário'
                else 'Sem Informação'
            end as cons_paritario,
            {{ eixo1_sim_nao("deliberativo") }} = 'Sim' as tem_deliberativo,
            {{ eixo1_sim_nao("consultivo") }} = 'Sim' as tem_consultivo,
            {{ eixo1_sim_nao("normativo") }} = 'Sim'
            or {{ eixo1_sim_nao("fiscalizador") }} = 'Sim' as tem_norm_fisc
        from conselhos_2021_2018
    ),

    classificado_2014 as (
        select
            a1::text as cod_municipio,
            2014 as ano,
            -- Em 2014 o R também conta "Não informado" como não paritário: o
            -- padrão dele procura "não".
            case
                when a307 ~* '[éÉ] paritário|^parit|sim|^1$'
                then 'Paritário'
                when a307 ~* 'maior representação|governamental|sociedade civil|não|^2$'
                then 'Não Paritário'
                else 'Sem Informação'
            end as cons_paritario,
            {{ eixo1_sim_nao("a309") }} = 'Sim' as tem_deliberativo,
            {{ eixo1_sim_nao("a308") }} = 'Sim' as tem_consultivo,
            {{ eixo1_sim_nao("a310") }} = 'Sim'
            or {{ eixo1_sim_nao("a311") }} = 'Sim' as tem_norm_fisc
        from {{ ref("silver_munic_cultura_2014_instancias_participativas") }}
        where {{ eixo1_sim_nao("a305") }} = 'Sim'
    ),

    conselhos as (
        select *
        from classificado_2014
        union all
        select *
        from classificado_2021_2018
    )

select
    cod_municipio,
    ano,
    cons_paritario,
    case
        when tem_deliberativo
        then 'Deliberativo (com ou sem consultivo)'
        when not tem_deliberativo and tem_consultivo
        then 'Apenas Consultivo'
        when not tem_deliberativo and not tem_consultivo and tem_norm_fisc
        then 'Normativo / Fiscalizador'
        else 'Sem Informação'
    end as cons_competencia
from conselhos
