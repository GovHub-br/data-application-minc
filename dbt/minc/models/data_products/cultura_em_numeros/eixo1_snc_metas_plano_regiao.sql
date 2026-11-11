-- Gold do Eixo 1, frente SNC: entre os municípios com plano de cultura
-- 'Concluída', quantos têm metas definidas, por região (gráfico 6.3 do SNC.R,
-- eda_9_metas_plano_vs_regiao). Só conta a resposta 'Sim' ou 'Não', como no R.
with

    contagem as (
        select regiao, possui_metas, count(*) as quantidade_municipios
        from {{ ref("eixo1_snc_painel_municipio") }}
        where situacao_plano = 'Concluída' and possui_metas in ('Sim', 'Não')
        group by regiao, possui_metas
    )

select
    regiao,
    possui_metas,
    quantidade_municipios,
    quantidade_municipios::numeric
    / sum(quantidade_municipios) over (partition by regiao) as proporcao
from contagem
