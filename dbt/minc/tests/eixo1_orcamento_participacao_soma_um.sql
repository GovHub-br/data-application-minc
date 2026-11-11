-- A participação de cada origem no investimento estadual do ano (Fig. 5)
-- soma 1 em todo exercício que tem valor real. Devolve os anos que não somam.
select exercicio, sum(participacao) as soma
from {{ ref("eixo1_orcamento_estadual_fonte") }}
where participacao is not null
group by exercicio
having abs(sum(participacao) - 1) > 1e-9
