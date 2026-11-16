-- Gold do Eixo 1, frente SNC: municípios com conselho, fundo e plano de cultura
-- 'Concluída', e com os três, por região (tabela da seção 8 do SNC.R,
-- Tabela_SNC_Tripe_Regiao_2026). Uma linha por região e componente; a proporção
-- é sobre todos os municípios da região na lista.
with

    componentes as (
        select regiao, 'Conselho' as componente, situacao_conselho = 'Concluída' as tem
        from {{ ref("eixo1_snc_painel_municipio") }}
        union all
        select regiao, 'Fundo' as componente, situacao_fundo = 'Concluída' as tem
        from {{ ref("eixo1_snc_painel_municipio") }}
        union all
        select regiao, 'Plano' as componente, situacao_plano = 'Concluída' as tem
        from {{ ref("eixo1_snc_painel_municipio") }}
        union all
        select regiao, 'Tripé completo' as componente, tripe_completo = 'Sim' as tem
        from {{ ref("eixo1_snc_painel_municipio") }}
    )

select
    regiao,
    componente,
    count(*) filter (where tem) as quantidade_municipios,
    count(*) as total_municipios,
    count(*) filter (where tem)::numeric / count(*) as proporcao
from componentes
where regiao is not null
group by regiao, componente
