-- Gold — a participação da cultura no orçamento dos municípios: o empenho
-- próprio em cultura como fração da Receita Corrente Líquida, e a faixa em que
-- cada município cai. É a base da Fig. 9 e dos Indicadores 1 a 4 do passo 8 de
-- gasto_municipal.R.
--
-- NUMERADOR: o empenho de origem 'proprio' (sem emenda, LPG nem PNAB), em
-- reais correntes, de eixo1_orcamento_municipal_municipio_fonte.
--
-- DENOMINADOR: a RCL do RREO, Anexo 03, 6º bimestre, coluna "TOTAL (ÚLTIMOS 12
-- MESES)", na conta de código RREO3ReceitaCorrenteLiquida ("RECEITA CORRENTE
-- LÍQUIDA (III)"). Vale o RREO completo e o simplificado: cerca de 2.200
-- municípios por ano entregam só o 'RREO Simplificado', que traz a RCL na
-- mesma conta e coluna. Nenhum município entrega os dois no mesmo ano, então a
-- linha por (exercicio, cod_ibge) continua única (teste no schema).
-- DIFERENTE DO R: o R pega o MAIOR valor entre as linhas cujo texto casa com
-- "RECEITA CORRENTE LÍQUIDA", o que inclui as duas RCL ajustadas. Nos RCL
-- municipais de 2018 a 2025 que o R versiona, isso dá outro valor em 17 de
-- 43.714 pares (ano, município), todos em 2020 e 2021, e nenhum muda de faixa.
--
-- FAIXA: o percentual da RCL, (próprio / RCL) * 100, como o R: até 0,5%, de
-- 0,5% a 1,0%, de 1,0% a 2,0% (cada uma fechada em cima) e mais de 2,0%. Só tem
-- faixa quem tem empenho próprio e RCL positiva, o mesmo universo do R: o
-- município sem empenho próprio em cultura não entra como "até 0,5%".
-- O DF fica fora, como no empenho.
-- GRÃO: uma linha por (exercicio, cod_ibge) do município que tem empenho
-- próprio em cultura ou RCL.
with
    proprio as (
        select exercicio, cod_ibge, sum(valor_nominal) as valor_proprio_nominal
        from {{ ref("eixo1_orcamento_municipal_municipio_fonte") }}
        where origem = 'proprio'
        group by exercicio, cod_ibge
    ),

    rcl as (
        select exercicio, cod_ibge, valor as rcl
        from {{ ref("silver_rreo") }}
        where
            cod_ibge between 1000000 and 9999999
            and demonstrativo in ('RREO', 'RREO Simplificado')
            and periodo = 6
            and anexo = 'RREO-Anexo 03'
            and cod_conta = 'RREO3ReceitaCorrenteLiquida'
            and coluna = 'TOTAL (ÚLTIMOS 12 MESES)'
    ),

    juntos as (
        select
            p.valor_proprio_nominal,
            r.rcl,
            coalesce(p.exercicio, r.exercicio) as exercicio,
            coalesce(p.cod_ibge, r.cod_ibge) as cod_ibge,
            case
                when r.rcl > 0 then p.valor_proprio_nominal / r.rcl
            end as participacao_rcl
        from proprio as p
        full outer join rcl as r on p.exercicio = r.exercicio and p.cod_ibge = r.cod_ibge
    )

select
    j.exercicio,
    j.cod_ibge,
    m.ente,
    m.sigla_uf as uf,
    j.valor_proprio_nominal,
    j.rcl,
    j.participacao_rcl,
    m.populacao_censo_2022,
    {{ siconfi_regiao_por_extenso("m.regiao") }} as regiao,
    {{ eixo1_porte_pnas("m.populacao_censo_2022") }} as porte,
    case
        when j.participacao_rcl * 100 <= 0.5
        then '1. Até 0,5%'
        when j.participacao_rcl * 100 <= 1.0
        then '2. Entre 0,5% e 1,0%'
        when j.participacao_rcl * 100 <= 2.0
        then '3. Entre 1,0% e 2,0%'
        when j.participacao_rcl * 100 > 2.0
        then '4. Mais de 2,0%'
    end as faixa_rcl,
    j.participacao_rcl * 100 > 2.0 as acima_de_2_porcento_rcl
from juntos as j
left join {{ ref("int_ibge_sidra_siconfi") }} as m on j.cod_ibge = m.cod_ibge
where m.sigla_uf is distinct from 'DF'
