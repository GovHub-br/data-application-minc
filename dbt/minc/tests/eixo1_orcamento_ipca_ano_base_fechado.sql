-- O ano-base do valor real do orçamento do Eixo 1 (var
-- eixo1_orcamento_ano_base_ipca) é um ano fechado do IPCA: a série 433 tem os
-- 12 meses dele. Com meses parciais (o ano corrente, por exemplo), o deflator
-- seria a média de parte do ano, e todo valor_real sairia enviesado sem erro.
--
-- Confere também a escala: a gold-base lê o índice do ano-base direto do IPCA
-- e o do exercício de int_bacen_siconfi. As duas contas são a mesma, então no
-- ano-base fator_ipca é 1; se uma delas mudar sozinha, deixa de ser.
-- Devolve uma linha por problema.
{% set ano_base = var("eixo1_orcamento_ano_base_ipca", 2024) %}

select 'ano-base sem os 12 meses do IPCA' as problema, count(*) as quantidade
from {{ ref("silver_serie_sgs") }}
where codigo_serie = '433' and extract(year from data) = {{ ano_base }}
having count(*) <> 12

union all

select 'fator_ipca diferente de 1 no ano-base' as problema, count(*) as quantidade
from {{ ref("eixo1_orcamento_empenho_cultura") }}
where exercicio = {{ ano_base }} and fator_ipca <> 1
having count(*) > 0
