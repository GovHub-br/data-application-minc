-- Gold do Eixo 1, frente orçamento, esfera federal: Figura 3 da publicação, a
-- participação da cultura no orçamento da União, com a LPG empenhada em 2022
-- contada em 2023 (gráfico IX do gasto_federal.R, grafico_rcl_lpg_2023, e o
-- CSV "Figura 3 – Participação da cultura no orçamento da União").
--
-- É eixo1_orcamento_federal_impacto_rcl com um ajuste: o empenho da Lei Paulo
-- Gustavo de 2022 sai de 2022 e entra em 2023, o ano em que foi executado,
-- como na Figura 1.
--
-- DIFERENTE DO R: nas linhas 461 a 468 do gasto_federal.R, o valor da LPG de
-- 2022 é calculado por linha com ifelse(ano == 2022, ..., 0). Na linha de 2023
-- ele vale 0, então o R tira a LPG de 2022 e não a soma em 2023: ela some da
-- série, e o numerador de 2023 fica R$ 3.862.000.000,00 menor que o da Figura
-- 1. Aqui ela entra em 2023. O de 2022 é igual ao do R.
--
-- Como em eixo1_orcamento_federal_impacto_rcl, o exercício sem RCL fica, com a
-- participação nula; hoje são os de 2018 em diante, 2022 e 2023 inclusive.
with

    lpg_2022 as (
        select coalesce(sum(empenhado), 0) as valor_empenhado
        from {{ ref("eixo1_orcamento_federal_empenho_cultura") }}
        where ano = 2022 and categoria_origem = 'Lei Paulo Gustavo (LPG)'
    ),

    ajustado as (
        select
            i.ano,
            i.valor_empenhado as valor_empenhado_exercicio,
            i.rcl,
            i.fonte_rcl,
            case
                when i.ano = 2022
                then -1 * l.valor_empenhado
                when i.ano = 2023
                then l.valor_empenhado
                else 0
            end as ajuste_lpg
        from {{ ref("eixo1_orcamento_federal_impacto_rcl") }} as i
        cross join lpg_2022 as l
    )

select
    ano,
    valor_empenhado_exercicio,
    ajuste_lpg,
    rcl,
    fonte_rcl,
    valor_empenhado_exercicio + ajuste_lpg as valor_empenhado,
    case
        when rcl > 0 then (valor_empenhado_exercicio + ajuste_lpg) / rcl
    end as participacao_rcl
from ajustado
