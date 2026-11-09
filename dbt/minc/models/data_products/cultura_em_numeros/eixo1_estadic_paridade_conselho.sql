-- Gold do Eixo 1, frente ESTADIC: a composição dos conselhos estaduais de
-- cultura em 2018 e 2021 (gráfico 5.3 do ESTADIC.R). Só as UFs com conselho e
-- composição informada.
with

    contagem as (
        select
            ano, trim(cons_paritario) as composicao_conselho, count(*) as quantidade_ufs
        from {{ ref("eixo1_estadic_painel_uf") }}
        where
            ano in (2018, 2021)
            and tem_conselho = 'Sim'
            and trim(cons_paritario) in ('Paritário', 'Não Paritário')
        group by ano, trim(cons_paritario)
    )

select
    ano,
    composicao_conselho,
    quantidade_ufs,
    quantidade_ufs::numeric / sum(quantidade_ufs) over (partition by ano) as proporcao
from contagem
