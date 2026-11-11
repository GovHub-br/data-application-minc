-- Gold do Eixo 1, frente SNC: municípios por natureza do conselho de cultura e
-- paridade (gráfico 5.3 do SNC.R, eda_3_natureza_paridade_conselhos). Como no R:
-- exige natureza e paridade preenchidas e tira só a paridade igual a 'Não
-- informado', valor que a Plataforma SNC não usa (ela escreve 'Não
-- informado(a)', que fica). Por isso a contagem inclui os municípios sem adesão
-- e os sem informação.
select natureza_conselho, conselho_paritario, count(*) as quantidade_municipios
from {{ ref("eixo1_snc_painel_municipio") }}
where
    natureza_conselho is not null
    and conselho_paritario is not null
    and conselho_paritario <> 'Não informado'
group by natureza_conselho, conselho_paritario
