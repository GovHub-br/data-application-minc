-- Gold do Eixo 1, frente ESTADIC: o tipo de órgão gestor da cultura nos estados
-- em 2014, 2018 e 2021 (gráfico 5.1 do ESTADIC.R), em três grupos.
with

    contagem as (
        select ano, tipo_orgao_gestor, count(*) as quantidade_ufs
        from {{ ref("eixo1_estadic_painel_uf") }}
        where ano in (2014, 2018, 2021) and tipo_orgao_gestor is not null
        group by ano, tipo_orgao_gestor
    )

select
    ano,
    tipo_orgao_gestor,
    quantidade_ufs,
    quantidade_ufs::numeric / sum(quantidade_ufs) over (partition by ano) as proporcao
from contagem
