-- Gold do Eixo 1, frente ESTADIC: a cultura nas políticas sociais dos estados,
-- pelos módulos transversais da ESTADIC 2023 (primeira infância e política para
-- mulheres) e 2024 (igualdade racial) (gráfico 6.2 do ESTADIC.R). Cada
-- indicador é a proporção de 'Sim' entre as UFs que responderam.
--
-- O patrimônio afro e o quilombola de 2024 foram retirados até a #176: o
-- ESTADIC.R lê para eles a eigr128 e a eigr1210, que são as áreas Trabalho e
-- Justiça no grupo de trabalho de igualdade racial (ver o schema.yml).
--
-- Sem filtro de ano: cada coluna só é preenchida na edição que a pergunta, e
-- nas outras fica NULL no painel, o que o "resposta is not null" já tira.
with

    indicadores as (
        select item.indicador, item.resposta
        from {{ ref("eixo1_estadic_painel_uf") }} as painel
        cross join
            lateral(  -- noqa: LT01
                values
                    (
                        'Arte e Cultura na Primeira Infância (2023)',
                        painel.pi_arte_cultura
                    ),
                    (
                        'Articulação Mulheres x Órgão de Cultura (2023)',
                        painel.pm_parceria_cultura
                    ),
                    ('Cultura em GTs de Igualdade Racial (2024)', painel.ir_gt_cultura)
            ) as item(indicador, resposta)  -- noqa: LT01
    )

select
    indicador,
    count(*) filter (where resposta = 'Sim') as quantidade_ufs_sim,
    count(*) as quantidade_ufs,
    avg(case when resposta = 'Sim' then 1 else 0 end) as proporcao
from indicadores
where resposta is not null
group by indicador
