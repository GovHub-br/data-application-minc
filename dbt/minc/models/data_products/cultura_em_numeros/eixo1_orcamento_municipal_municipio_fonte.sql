-- Gold — empenho municipal em cultura por exercício, município e origem do
-- recurso, em reais correntes e de 2024, com a população do Censo 2022 e o
-- porte do município. É a municipal_final.csv de gasto_municipal.R, sem a LAB 1
-- (ver abaixo).
--
-- Agrega eixo1_orcamento_empenho_cultura, onde está a regra (empenho, origem,
-- função 13, DF fora). O município é o cod_ibge do item da MSC: o R casa o
-- nome do município com a malha do IBGE, e o que não casa fica sem código.
--
-- PORTE: as faixas da PNAS (macro eixo1_porte_pnas) sobre a população
-- residente do Censo 2022 (int_ibge_sidra_siconfi), as mesmas da
-- MUNIC_FINAL.xlsx que o R lê. Sem população do Censo, o porte fica nulo.
--
-- LAB 1: no R, a origem 'Lei Aldir Blanc 1 (LAB 1)' do município vem do cubo
-- do BB Ágil, não da MSC (a MSC não separa a LAB 1 na fonte). Ainda não entra
-- aqui: o extrato do BB Ágil no banco começa em 2023.
-- GRÃO: uma linha por (exercicio, cod_ibge, origem) que tem empenho.
with
    por_origem as (
        select
            exercicio,
            cod_ibge,
            origem,
            sum(valor_nominal) as valor_nominal,
            sum(valor_real) as valor_real,
            count(*) as linhas_msc
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
    p.linhas_msc,
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
