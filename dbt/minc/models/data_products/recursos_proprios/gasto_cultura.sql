-- Gold de recursos_proprios: gasto_cultura, quanto cada ente empenhou na
-- função Cultura (13) em cada ano, em reais correntes.
-- GRÃO: 1 linha por ente (cod_ibge) e exercício.
--
-- Regras (o núcleo de gasto_estadual.R e gasto_municipal.R, repositório
-- cultura-em-numeros-eixo-1):
-- 1. Fonte: a MSC orçamentária do SICONFI, já deduplicada na Silver.
-- 2. O acumulado do ano: matriz agregada (MSCC), mês 12, classe 6, saldo
-- final. Ano sem a MSC de dezembro ainda não entra.
-- 3. Gasto é crédito empenhado: natureza do saldo credora (C) e conta
-- contábil começando com 62213.
-- 4. Cultura é a função 13.
-- 5. O valor em módulo, linha a linha, como o R.
--
-- Fica de fora, e está na Gold completa eixo1_orcamento_empenho_cultura: a
-- origem do recurso (PNAB, LPG, emenda), a LAB 1 municipal do BB Ágil e o
-- valor deflacionado pelo IPCA.
with
    empenho_cultura as (
        select cod_ibge, exercicio, abs(valor) as valor
        from {{ ref("silver_msc_orcamentaria") }}
        where
            tipo_matriz = 'MSCC'
            and mes_referencia = 12
            and classe_conta = 6
            and tipo_valor = 'ending_balance'
            and natureza_conta = 'C'
            and replace(conta_contabil, '.', '') like '62213%'
            and substring(funcao from '[0-9]+')::integer = 13
    ),

    por_ente_ano as (
        select cod_ibge, exercicio, sum(valor) as gasto_cultura, count(*) as linhas_msc
        from empenho_cultura
        group by cod_ibge, exercicio
    )

select
    g.exercicio,
    g.cod_ibge,
    e.ente,
    e.sigla_uf,
    e.regiao,
    g.gasto_cultura,
    g.linhas_msc,
    case
        when g.cod_ibge between 10 and 99
        then 'estadual'
        when g.cod_ibge between 1000000 and 9999999
        then 'municipal'
    end as esfera
from por_ente_ano as g
left join {{ ref("silver_entes") }} as e on g.cod_ibge = e.cod_ibge
