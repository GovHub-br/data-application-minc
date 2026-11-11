-- As duas proporções do municipal somam 1: a participação de cada origem no
-- investimento do ano (Fig. 8) e a proporção de municípios por faixa em cada
-- (exercício, região) (Fig. 9). Devolve os grupos que não somam.
select 'fig8' as figura, exercicio, null as regiao, sum(participacao) as soma
from {{ ref("eixo1_orcamento_municipal_fonte") }}
where participacao is not null
group by exercicio
having abs(sum(participacao) - 1) > 1e-9

union all

select 'fig9' as figura, exercicio, regiao, sum(proporcao_municipios) as soma
from {{ ref("eixo1_orcamento_municipal_faixa_rcl") }}
group by exercicio, regiao
having abs(sum(proporcao_municipios) - 1) > 1e-9
