-- Gold do Eixo 1, frente SNC: municípios aderidos ao SNC por região, em número e
-- em cobertura (gráfico 5.5 do SNC.R, eda_5_adesao_regiao_absoluto). Aderido é
-- 'Publicado no DOU'; a cobertura é sobre todos os municípios da região na lista.
select
    regiao,
    count(*) as quantidade_municipios,
    count(*) filter (where adesao_publicada) as adesoes,
    count(*) filter (where adesao_publicada)::numeric / count(*) as taxa_cobertura
from {{ ref("eixo1_snc_painel_municipio") }}
where regiao is not null
group by regiao
