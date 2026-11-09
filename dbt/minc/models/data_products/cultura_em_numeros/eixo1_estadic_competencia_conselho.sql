-- Gold do Eixo 1, frente ESTADIC: a competência dos conselhos estaduais de
-- cultura (gráfico 5.4 do ESTADIC.R). Só as UFs com conselho e competência
-- informada; a ESTADIC 2014 não pergunta a competência, então só sobram 2018 e
-- 2021, como no R.
with

    contagem as (
        select ano, cons_competencia, count(*) as quantidade_ufs
        from {{ ref("eixo1_estadic_painel_uf") }}
        where
            ano in (2014, 2018, 2021)
            and tem_conselho = 'Sim'
            and cons_competencia <> 'Sem Informação'
        group by ano, cons_competencia
    )

select
    ano,
    cons_competencia,
    quantidade_ufs,
    quantidade_ufs::numeric / sum(quantidade_ufs) over (partition by ano) as proporcao
from contagem
