-- Gold do Eixo 1, frente ESTADIC: a cultura nas políticas sociais dos estados,
-- pelos módulos transversais da ESTADIC 2023 (primeira infância e política para
-- mulheres) e 2024 (igualdade racial) (gráfico 6.2 do ESTADIC.R). Cada
-- indicador é a proporção de 'Sim' entre as UFs que responderam.
with

    indicadores as (
        select
            'Arte e Cultura na Primeira Infância (2023)' as indicador,
            pi_arte_cultura as resposta
        from {{ ref("eixo1_estadic_painel_uf") }}
        where ano in (2023, 2024)
        union all
        select
            'Articulação Mulheres x Órgão de Cultura (2023)' as indicador,
            pm_parceria_cultura as resposta
        from {{ ref("eixo1_estadic_painel_uf") }}
        where ano in (2023, 2024)
        union all
        select
            'Cultura em GTs de Igualdade Racial (2024)' as indicador,
            ir_gt_cultura as resposta
        from {{ ref("eixo1_estadic_painel_uf") }}
        where ano in (2023, 2024)
        union all
        select
            'Preservação Patrimônio Afro-Brasileiro (2024)' as indicador,
            ir_patrimonio_afro as resposta
        from {{ ref("eixo1_estadic_painel_uf") }}
        where ano in (2023, 2024)
        union all
        select
            'Preservação Patrimônio Quilombola (2024)' as indicador,
            ir_patrimonio_quilombola as resposta
        from {{ ref("eixo1_estadic_painel_uf") }}
        where ano in (2023, 2024)
    )

select
    indicador,
    count(*) filter (where resposta = 'Sim') as quantidade_ufs_sim,
    count(*) as quantidade_ufs,
    avg(case when resposta = 'Sim' then 1 else 0 end) as proporcao
from indicadores
where resposta is not null
group by indicador
