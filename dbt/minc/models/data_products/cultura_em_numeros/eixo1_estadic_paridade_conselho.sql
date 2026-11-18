-- Gold do Eixo 1, frente ESTADIC: a composição dos conselhos estaduais de
-- cultura em 2018 e 2021 (gráfico 5.3 do ESTADIC.R). Só as UFs com conselho e
-- composição informada. Sem filtro de ano: nas edições que não perguntam a
-- composição ela fica NULL no painel.
with

    contagem as (
        select ano, cons_paritario as composicao_conselho, count(*) as quantidade_ufs
        from {{ ref("eixo1_estadic_painel_uf") }}
        where tem_conselho = 'Sim' and cons_paritario in ('Paritário', 'Não Paritário')
        group by ano, cons_paritario
    )

select
    ano,
    composicao_conselho,
    quantidade_ufs,
    quantidade_ufs::numeric / sum(quantidade_ufs) over (partition by ano) as proporcao
from contagem
