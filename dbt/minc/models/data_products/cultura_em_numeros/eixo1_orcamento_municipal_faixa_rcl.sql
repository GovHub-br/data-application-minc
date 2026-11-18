-- Gold — Fig. 9 da publicação: o percentual de municípios em cada faixa de
-- destinação da RCL à cultura, por exercício e grande região.
--
-- Conta os municípios de eixo1_orcamento_municipal_rcl que têm faixa (empenho
-- próprio em cultura e RCL positiva). A proporção é dentro do (exercício,
-- região), como no R. É também o Indicador_3 de gasto_municipal.R, somando as
-- regiões.
--
-- A região vem de um left join em eixo1_orcamento_municipal_rcl: o município
-- fora de silver_entes fica sem ela. Ele não entra aqui, em vez de virar uma
-- região nula com proporção própria; o not_null de regiao em
-- eixo1_orcamento_municipal_rcl (nas linhas com faixa) faz o build acusar.
-- GRÃO: uma linha por (exercicio, regiao, faixa_rcl) com município.
select
    exercicio,
    regiao,
    faixa_rcl,
    count(*) as quantidade_municipios,
    count(*)::numeric
    / sum(count(*)) over (partition by exercicio, regiao) as proporcao_municipios
from {{ ref("eixo1_orcamento_municipal_rcl") }}
where faixa_rcl is not null and regiao is not null
group by exercicio, regiao, faixa_rcl
