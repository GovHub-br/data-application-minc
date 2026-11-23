-- A gasto_cultura (produto recursos_proprios) e a eixo1_orcamento_empenho_cultura
-- (produto cultura_em_numeros) escrevem a mesma regra do empenho em Cultura na
-- MSC: MSCC de dezembro, classe 6, saldo final, natureza C, conta 62213,
-- função 13, valor em módulo, sem a União e com o DF só como estado. A Gold de
-- um produto não lê a do outro (ADR 0015, decisão 4), então a regra está nos
-- dois lugares, e este teste é o que impede as duas de divergirem em silêncio
-- na primeira correção.
--
-- Do lado da eixo1 entra só o ramo da MSC (base_origem 'siconfi_msc') na
-- função 13: as linhas da LAB estadual fora da função 13 e a LAB 1 municipal do
-- BB Ágil não existem na gasto_cultura. A soma é do valor_nominal, em reais
-- correntes, por ente e exercício, de todas as origens.
--
-- Devolve os pares de ente e exercício que só existem de um lado ou cuja soma
-- difere em mais de um centavo.
with
    eixo1 as (
        select cod_ibge, exercicio, sum(valor_nominal) as gasto_cultura
        from {{ ref("eixo1_orcamento_empenho_cultura") }}
        where
            base_origem = 'siconfi_msc'
            -- o critério da própria eixo1: se a funcao vier noutro formato que
            -- não '13', a gasto_cultura perde a linha e o teste acusa
            and substring(funcao from '[0-9]+')::integer = 13
        group by cod_ibge, exercicio
    ),

    recursos_proprios as (
        select cod_ibge, exercicio, gasto_cultura from {{ ref("gasto_cultura") }}
    )

select
    r.gasto_cultura as gasto_recursos_proprios,
    e.gasto_cultura as gasto_eixo1,
    coalesce(r.cod_ibge, e.cod_ibge) as cod_ibge,
    coalesce(r.exercicio, e.exercicio) as exercicio
from recursos_proprios as r
full outer join eixo1 as e on r.cod_ibge = e.cod_ibge and r.exercicio = e.exercicio
where
    r.cod_ibge is null
    or e.cod_ibge is null
    or abs(r.gasto_cultura - e.gasto_cultura) > 0.01
