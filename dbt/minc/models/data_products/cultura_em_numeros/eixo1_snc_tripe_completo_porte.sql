-- Gold do Eixo 1, frente SNC: municípios com e sem o tripé completo (conselho,
-- fundo e plano 'Concluída') por faixa populacional (gráfico 6.2 do SNC.R,
-- eda_8_tripe_completo_vs_porte), nas cinco faixas. A proporção é dentro da
-- faixa. O tripé que a gold-base deixa NULL (falta situação e as conhecidas são
-- 'Concluída') entra como 'Não', o que o R dá a ele.
with

    contagem as (
        select
            faixa_populacional,
            coalesce(tripe_completo, 'Não') as tripe_completo,
            count(*) as quantidade_municipios
        from {{ ref("eixo1_snc_painel_municipio") }}
        where faixa_populacional is not null
        group by faixa_populacional, coalesce(tripe_completo, 'Não')
    )

select
    faixa_populacional,
    tripe_completo,
    quantidade_municipios,
    quantidade_municipios::numeric
    / sum(quantidade_municipios) over (partition by faixa_populacional) as proporcao
from contagem
