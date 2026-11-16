-- Gold do Eixo 1, frente SNC: a situação declarada do conselho, do fundo e do
-- plano de cultura nos municípios (gráfico 5.2 do SNC.R, eda_2_status_tripe). A
-- proporção é dentro do instrumento. Situação ausente entra como 'Não
-- Informado/Em Branco', o rótulo que o R dá a ela no gráfico.
with

    instrumentos as (
        select 'Conselho de Cultura' as instrumento, situacao_conselho as situacao
        from {{ ref("eixo1_snc_painel_municipio") }}
        union all
        select 'Fundo de Cultura' as instrumento, situacao_fundo as situacao
        from {{ ref("eixo1_snc_painel_municipio") }}
        union all
        select 'Plano de Cultura' as instrumento, situacao_plano as situacao
        from {{ ref("eixo1_snc_painel_municipio") }}
    ),

    contagem as (
        select
            instrumento,
            coalesce(situacao, 'Não Informado/Em Branco') as situacao,
            count(*) as quantidade_municipios
        from instrumentos
        group by instrumento, coalesce(situacao, 'Não Informado/Em Branco')
    )

select
    instrumento,
    situacao,
    quantidade_municipios,
    quantidade_municipios::numeric
    / sum(quantidade_municipios) over (partition by instrumento) as proporcao
from contagem
