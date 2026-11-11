-- Gold do Eixo 1, frente ESTADIC: os estados com lei de incentivo à cultura
-- própria em 2018 e 2021 (gráfico 6.4 do ESTADIC.R, salvo como grafico_7_2).
with

    contagem as (
        select ano, tem_lei_incentivo, count(*) as quantidade_ufs
        from {{ ref("eixo1_estadic_painel_uf") }}
        where ano in (2018, 2021) and tem_lei_incentivo is not null
        group by ano, tem_lei_incentivo
    )

select
    ano,
    tem_lei_incentivo,
    quantidade_ufs,
    quantidade_ufs::numeric / sum(quantidade_ufs) over (partition by ano) as proporcao
from contagem
