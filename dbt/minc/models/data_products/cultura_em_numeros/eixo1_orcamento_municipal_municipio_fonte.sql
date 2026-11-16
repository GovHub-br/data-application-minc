-- Gold — empenho municipal em cultura por exercício, município e origem do
-- recurso, em reais correntes e de 2024, com a população do Censo 2022 e o
-- porte do município. É a municipal_final.csv de gasto_municipal.R.
--
-- Agrega eixo1_orcamento_empenho_cultura, onde está a regra (empenho, origem,
-- função 13, DF fora da MSC, LAB 1 do extrato do BB Ágil). O município é o
-- cod_ibge do item da MSC ou o da conta do BB Ágil: o R casa o nome do
-- município da MSC com a malha do IBGE, e o que não casa fica sem código.
--
-- PORTE: as faixas da PNAS (macro eixo1_porte_pnas) sobre a população
-- residente do Censo 2022 (int_ibge_sidra_siconfi), as mesmas da
-- MUNIC_FINAL.xlsx que o R lê. Sem população do Censo, o porte fica nulo.
--
-- LAB 1: a origem 'lab', 'Lei Aldir Blanc 1 (LAB 1)', vem do extrato do BB
-- Ágil, como no R, que a tira do cubo do BB Ágil (a MSC não separa a LAB 1 na
-- fonte do município). O DF entra só aqui, como Brasília (5300108).
-- GRÃO: uma linha por (exercicio, cod_ibge, origem) que tem empenho.
with
    por_origem as (
        select
            exercicio,
            cod_ibge,
            origem,
            sum(valor_nominal) as valor_nominal,
            sum(valor_real) as valor_real,
            count(*) as linhas
        from {{ ref("eixo1_orcamento_empenho_cultura") }}
        where esfera_eixo1 = 'municipal'
        group by exercicio, cod_ibge, origem
    )

select
    p.exercicio,
    p.cod_ibge,
    m.ente,
    m.sigla_uf as uf,
    p.origem,
    p.valor_nominal,
    p.valor_real,
    p.linhas,
    m.populacao_censo_2022,
    {{ siconfi_regiao_por_extenso("m.regiao") }} as regiao,
    case
        p.origem
        when 'proprio'
        then 'Recurso Próprio (Municipal)'
        when 'emenda'
        then 'Emendas Parlamentares (Cultura)'
        when 'lab'
        then 'Lei Aldir Blanc 1 (LAB 1)'
        when 'lpg'
        then 'Lei Paulo Gustavo (LPG)'
        when 'pnab'
        then 'PNAB (Aldir Blanc 2)'
    end as fonte_recurso,
    {{ eixo1_porte_pnas("m.populacao_censo_2022") }} as porte
from por_origem as p
left join {{ ref("int_ibge_sidra_siconfi") }} as m on p.cod_ibge = m.cod_ibge
