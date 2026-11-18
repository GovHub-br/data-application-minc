-- Gold do Eixo 1, frente SNC: conselhos, fundos e planos de cultura municipais
-- criados por ano, pelo ano da lei (do plano, no caso do plano). São as Figuras
-- 12, 13 e 14 da publicação (seção 7 do snc_figuras_tabelas.R) e o eda_10 do
-- SNC.R (seção 7, a evolução das adesões validadas do tripé).
--
-- Como nas Figuras 12 a 14: só municípios; o componente conta com situação
-- 'Concluída', aderido o município ao SNC ou não (o R não filtra a adesão aqui);
-- sem ano da lei, fica de fora. Cada componente tem todos os anos entre o
-- primeiro e o último com lei, com zero onde nenhum foi criado (o
-- complete(full_seq) do R).
--
-- O eda_10 é esta série cortada entre 2012 e 2026, com o acumulado somado desde
-- o primeiro ano, antes do corte, como aqui; lá ano sem lei nova não aparece,
-- e aqui aparece com zero. O eda_10 converte a data como número de série do
-- Excel, o formato da planilha que o SNC.R lê; no portal ela vem em texto
-- dd/mm/aaaa, e vale o ano da data, como nas Figuras 12 a 14.
with

    criados as (
        select componente, ano_lei as ano_criacao, count(*) as quantidade_criados
        from {{ ref("eixo1_snc_componente_ente") }}
        where
            esfera = 'municipal'
            and componente in ('Conselho', 'Fundo', 'Plano')
            and tem_componente
            and ano_lei is not null
        group by componente, ano_lei
    ),

    limites as (
        select
            componente, min(ano_criacao) as primeiro_ano, max(ano_criacao) as ultimo_ano
        from criados
        group by componente
    ),

    anos as (
        select componente, generate_series(primeiro_ano, ultimo_ano) as ano_criacao
        from limites
    ),

    serie as (
        select
            a.componente,
            a.ano_criacao,
            coalesce(c.quantidade_criados, 0) as quantidade_criados
        from anos as a
        left join
            criados as c on a.componente = c.componente and a.ano_criacao = c.ano_criacao
    )

select
    componente,
    ano_criacao,
    quantidade_criados,
    cast(
        sum(quantidade_criados) over (
            partition by componente order by ano_criacao
        ) as bigint
    ) as quantidade_acumulada
from serie
