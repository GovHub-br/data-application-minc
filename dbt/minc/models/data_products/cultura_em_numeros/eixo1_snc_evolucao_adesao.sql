-- Gold do Eixo 1, frente SNC: novas adesões ao SNC por ano e o acumulado
-- (gráfico 7.4 do SNC.R, eda_11_evolucao_geral_snc). É a Figura 9 que o SNC.R
-- grava (Figura 11 na publicação); as colunas têm os nomes do CSV dela.
--
-- Como no R: conta o município 'Publicado no DOU' com data de adesão, acumula
-- desde o primeiro ano e só depois corta em 2012–2026, e ano sem adesão nova
-- não aparece.
with

    novas as (
        select ano_adesao, count(*) as novas_adesoes
        from {{ ref("eixo1_snc_painel_municipio") }}
        where adesao_publicada and ano_adesao is not null
        group by ano_adesao
    ),

    acumuladas as (
        select
            ano_adesao,
            novas_adesoes,
            cast(
                sum(novas_adesoes) over (order by ano_adesao) as bigint
            ) as adesoes_acumuladas
        from novas
    )

select ano_adesao, 'Adesões acumuladas' as tipo_metrica, adesoes_acumuladas as quantidade
from acumuladas
where ano_adesao between 2012 and 2026
union all
select ano_adesao, 'Novas adesões no ano' as tipo_metrica, novas_adesoes as quantidade
from acumuladas
where ano_adesao between 2012 and 2026
