-- Gold do Eixo 1, frente SNC: o perfil do órgão gestor de cultura por faixa
-- populacional do município (gráfico 5.4 do SNC.R, eda_4_orgao_gestor_porte),
-- nas cinco faixas da planilha. A proporção é dentro da faixa.
with

    contagem as (
        select faixa_populacional, perfil_orgao_gestor, count(*) as quantidade_municipios
        from {{ ref("eixo1_snc_painel_municipio") }}
        where faixa_populacional is not null and perfil_orgao_gestor is not null
        group by faixa_populacional, perfil_orgao_gestor
    )

select
    faixa_populacional,
    perfil_orgao_gestor,
    quantidade_municipios,
    quantidade_municipios::numeric
    / sum(quantidade_municipios) over (partition by faixa_populacional) as proporcao
from contagem
