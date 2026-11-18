-- A LPG empenhada em 2022 tem de aparecer no ajuste da Figura 3: negativa em
-- 2022 e positiva em 2023. A gold acha a LPG pelo rótulo 'Lei Paulo Gustavo
-- (LPG)' da gold-base; se o rótulo mudar lá, lpg_2022 vira 0, o ajuste some
-- dos dois anos e eixo1_orcamento_federal_lpg_2023_soma_zero continua
-- passando. Este teste falha nesse caso, e também se 2022 ou 2023 faltarem.
with
    esperado as (
        select 2022 as ano
        union all
        select 2023 as ano
    )

select e.ano, f.ajuste_lpg
from esperado as e
left join {{ ref("eixo1_orcamento_federal_rcl_lpg_2023") }} as f on e.ano = f.ano
where
    f.ajuste_lpg is null
    or (e.ano = 2022 and f.ajuste_lpg >= 0)
    or (e.ano = 2023 and f.ajuste_lpg <= 0)
