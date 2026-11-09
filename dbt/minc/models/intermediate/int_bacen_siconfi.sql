-- Intermediate — MSC orçamentária do SICONFI com o ente e o IPCA do ano.
--
-- Cruza duas fontes e não decide nada (ADR 0010, regra 2; ADR 0015, decisão 3):
-- • siconfi: cada linha de silver_msc_orcamentaria, sem filtro, com o nome,
-- a UF, a esfera e a região do ente (silver_entes, pelo cod_ibge do item;
-- a MSC não traz a UF);
-- • bacen: o IPCA (SGS 433, silver_serie_sgs) do exercício da linha, como
-- número-índice médio do ano.
--
-- O NÚMERO-ÍNDICE. A série 433 é a variação mensal em %. O índice encadeia as
-- variações desde o primeiro mês da série (produto de 1 + v/100) e a média do
-- ano é a média dos meses daquele ano que a série tem. O primeiro mês é uma
-- escala arbitrária: some na razão entre dois anos, que é o que a Gold usa para
-- deflacionar. A escolha do ano-base é da Gold. ipca_meses_ano diz quantos
-- meses entraram na média (12 no ano fechado).
--
-- VIEW, NÃO TABLE: a linha é a da MSC inteira, de todos os entes e recortes que
-- a bronze tiver. Materializar duplicaria a silver; a Gold que filtra é que
-- vira tabela.
--
-- O ente e o IPCA entram como CTE `materialized`, calculadas uma vez por
-- consulta. Os filtros da Gold descem até a MSC, o Postgres estima uma linha e
-- junta por nested loop; sem isso, refaria a silver_entes (e a série do IPCA)
-- para cada linha da MSC. Mesma razão da macro siconfi_tentativa_valida.
-- (O noqa: LT01 nelas é porque o sqlfmt cola o parêntese no `materialized`.)
{{ config(materialized="view") }}

with
    ipca_mensal as (
        select
            data,
            exp(
                sum(ln(1 + valor / 100)) over (
                    order by data rows between unbounded preceding and current row
                )
            ) as indice
        from {{ ref("silver_serie_sgs") }}
        where codigo_serie = '433'
    ),

    ipca_ano as materialized(  -- noqa: LT01
        select
            extract(year from data)::integer as exercicio,
            avg(indice) as ipca_indice_medio_ano,
            count(*) as ipca_meses_ano
        from ipca_mensal
        group by 1
    ),

    ente as materialized(  -- noqa: LT01
        select cod_ibge, ente, sigla_uf, esfera, regiao from {{ ref("silver_entes") }}
    )

select
    msc.cod_ibge,
    ente.ente,
    ente.sigla_uf as uf,
    ente.esfera,
    ente.regiao,
    msc.exercicio,
    msc.mes_referencia,
    msc.data_referencia,
    msc.tipo_matriz,
    msc.classe_conta,
    msc.tipo_valor,
    msc.conta_contabil,
    msc.natureza_conta,
    msc.poder_orgao,
    msc.funcao,
    msc.subfuncao,
    msc.educacao_saude,
    msc.natureza_despesa,
    msc.natureza_receita,
    msc.ano_fonte_recursos,
    msc.fonte_recursos,
    msc.complemento_fonte,
    msc.ano_inscricao,
    msc.entrada_msc,
    msc.valor,
    ipca.ipca_indice_medio_ano,
    ipca.ipca_meses_ano,
    msc._pagina_id,
    msc._item
from {{ ref("silver_msc_orcamentaria") }} as msc
left join ente on msc.cod_ibge = ente.cod_ibge
left join ipca_ano as ipca on msc.exercicio = ipca.exercicio
