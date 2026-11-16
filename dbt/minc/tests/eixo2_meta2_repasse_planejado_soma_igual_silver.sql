-- O agregado do repasse planejado da Meta 2 soma o mesmo que a silver dos planos
-- de ação nos cinco programas da PNAB, em quantidade de planos e em valor. Uma
-- linha aqui é plano que caiu fora de algum grupo ou foi contado duas vezes.
with

    silver as (
        select count(*) as planos, sum(valor_total_repasse_plano_acao) as repasse
        from {{ ref("silver_plano_acao_minc") }}
        where id_programa in (60, 61, 62, 111, 112)
    ),

    gold as (
        select
            sum(quantidade_planos) as planos,
            sum(valor_total_repasse_planejado) as repasse
        from {{ ref("eixo2_meta2_repasse_planejado") }}
    )

select
    silver.planos as planos_silver,
    gold.planos as planos_gold,
    silver.repasse as repasse_silver,
    gold.repasse as repasse_gold
from silver
cross join gold
where
    silver.planos is distinct from gold.planos
    or silver.repasse is distinct from gold.repasse
