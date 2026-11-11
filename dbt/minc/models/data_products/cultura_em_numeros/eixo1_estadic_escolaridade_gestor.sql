-- Gold do Eixo 1, frente ESTADIC: a escolaridade do gestor estadual de cultura
-- em 2014, 2018 e 2021 (gráfico 5.2 do ESTADIC.R). Fora a resposta que não cabe
-- em nenhum grupo ('Sem Informação').
with

    contagem as (
        select ano, gestor_escolaridade_agrupada, count(*) as quantidade_ufs
        from {{ ref("eixo1_estadic_painel_uf") }}
        where
            ano in (2014, 2018, 2021)
            and gestor_escolaridade_agrupada is not null
            and gestor_escolaridade_agrupada <> 'Sem Informação'
        group by ano, gestor_escolaridade_agrupada
    )

select
    ano,
    gestor_escolaridade_agrupada,
    quantidade_ufs,
    quantidade_ufs::numeric / sum(quantidade_ufs) over (partition by ano) as proporcao
from contagem
