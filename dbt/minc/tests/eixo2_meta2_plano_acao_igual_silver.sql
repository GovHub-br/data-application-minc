-- A gold-base da Meta 2 não filtra situação: em cada programa da PNAB ela tem as
-- mesmas linhas e o mesmo repasse planejado que a silver. Uma linha aqui é plano
-- perdido no caminho ou duplicado pelo join com os relatórios de gestão.
with

    silver as (
        select
            id_programa,
            count(*) as linhas,
            sum(valor_total_repasse_plano_acao) as repasse
        from {{ ref("silver_plano_acao_minc") }}
        where id_programa in (60, 61, 62, 111, 112)
        group by id_programa
    ),

    gold as (
        select
            id_programa,
            count(*) as linhas,
            sum(valor_total_repasse_plano_acao) as repasse
        from {{ ref("eixo2_meta2_plano_acao") }}
        group by id_programa
    )

select
    silver.linhas as linhas_silver,
    gold.linhas as linhas_gold,
    silver.repasse as repasse_silver,
    gold.repasse as repasse_gold,
    coalesce(silver.id_programa, gold.id_programa) as id_programa
from silver
full outer join gold on silver.id_programa = gold.id_programa
where
    silver.linhas is distinct from gold.linhas
    or silver.repasse is distinct from gold.repasse
