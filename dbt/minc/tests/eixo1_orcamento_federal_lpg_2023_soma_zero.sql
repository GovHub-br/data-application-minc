-- A Figura 3 só muda o empenho da LPG de ano: o que sai de 2022 entra em 2023.
-- A soma dos ajustes é zero. Se 2023 faltasse na gold, ou se o ajuste voltasse
-- a ser calculado por linha como no R (ifelse(ano == 2022, ..., 0)), a LPG
-- sairia de 2022 sem entrar em 2023, e a soma ficaria negativa.
select sum(ajuste_lpg) as soma_ajustes
from {{ ref("eixo1_orcamento_federal_rcl_lpg_2023") }}
having sum(ajuste_lpg) <> 0
