-- Gold — Fig. 6 da publicação: a participação da cultura no orçamento dos
-- estados, o empenho próprio em cultura como fração da Receita Corrente
-- Líquida, contra a meta de 1,5% do PNC.
--
-- NUMERADOR: o empenho de origem 'proprio' (recurso do próprio estado, sem
-- emenda, LAB, LPG nem PNAB), em reais correntes, de
-- eixo1_orcamento_estadual_uf_fonte.
--
-- DENOMINADOR: a RCL do RREO, Anexo 03, 6º bimestre, coluna "TOTAL (ÚLTIMOS 12
-- MESES)", na conta de código RREO3ReceitaCorrenteLiquida ("RECEITA CORRENTE
-- LÍQUIDA (III)"). O filtro é pelo código, não pelo texto da linha: o texto
-- também casa com as RCL ajustadas, que têm outro código.
--
-- DIFERENTE DO R: onde falta o numerador ou a RCL, gasto_estadual.R põe 0% e
-- aqui fica nulo (ADR 0010, regra 1). É o caso do Ceará em 2019 a 2021 e de
-- Roraima em 2022, cuja MSC não informa a função da despesa: não é que o
-- estado não gastou com cultura, é que a MSC não diz. Os anos também não são cortados:
-- o R mostra
-- 2019 a 2024; aqui entra todo exercício com MSC ou RCL.
-- GRÃO: uma linha por (exercicio, cod_ibge) do estado.
with
    proprio as (
        select exercicio, cod_ibge, sum(valor_nominal) as valor_proprio_nominal
        from {{ ref("eixo1_orcamento_estadual_uf_fonte") }}
        where origem = 'proprio'
        group by exercicio, cod_ibge
    ),

    rcl as (
        select exercicio, cod_ibge, valor as rcl
        from {{ ref("silver_rreo") }}
        where
            cod_ibge between 10 and 99
            and demonstrativo = 'RREO'
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
            coalesce(p.cod_ibge, r.cod_ibge) as cod_ibge
        from proprio as p
        full outer join rcl as r on p.exercicio = r.exercicio and p.cod_ibge = r.cod_ibge
    )

select
    j.exercicio,
    j.cod_ibge,
    e.sigla_uf as uf,
    j.valor_proprio_nominal,
    j.rcl,
    {{ siconfi_regiao_por_extenso("e.regiao") }} as regiao,
    case when j.rcl > 0 then j.valor_proprio_nominal / j.rcl end as participacao_rcl,
    case
        when j.rcl > 0 then j.valor_proprio_nominal / j.rcl >= 0.015
    end as atinge_meta_pnc
from juntos as j
left join {{ ref("silver_entes") }} as e on j.cod_ibge = e.cod_ibge
