-- Gold do Eixo 1, frente SNC: a taxa de adesão ao SNC por porte do município, com
-- os portes 4 e 5 juntos (gráfico 5.6 do SNC.R, eda_6_adesao_porte_percentual).
-- É a Figura 8 que o SNC.R grava (Figura 10 na publicação); as colunas têm os
-- nomes do CSV dela.
select
    porte_populacional,
    count(*) as total_municipios,
    count(*) filter (where adesao_publicada) as adesoes_validadas,
    count(*) filter (where adesao_publicada)::numeric / count(*) as taxa_cobertura
from {{ ref("eixo1_snc_painel_municipio") }}
where porte_populacional is not null
group by porte_populacional
