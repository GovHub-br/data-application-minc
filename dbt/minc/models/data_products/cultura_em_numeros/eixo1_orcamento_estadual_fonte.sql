-- Gold — Figs. 4 e 5 da publicação: o investimento estadual em cultura por
-- exercício e origem do recurso (Fig. 4, em reais correntes) e a participação
-- de cada origem no total do ano (Fig. 5).
--
-- Como nas figuras de gasto_estadual.R, as emendas parlamentares ficam de fora
-- das duas: não somam no valor nem no total da participação. Estão em
-- eixo1_orcamento_estadual_uf_fonte.
--
-- A participação é sobre o valor real, como no R. Dentro de um ano todas as
-- origens levam o mesmo fator do IPCA, então ela é igual à do valor nominal.
-- GRÃO: uma linha por (exercicio, origem) que tem empenho.
with
    por_origem as (
        select
            exercicio,
            origem,
            fonte_recurso,
            sum(valor_nominal) as valor_nominal,
            sum(valor_real) as valor_real
        from {{ ref("eixo1_orcamento_estadual_uf_fonte") }}
        where origem <> 'emenda'
        group by exercicio, origem, fonte_recurso
    )

select
    exercicio,
    origem,
    fonte_recurso,
    valor_nominal,
    valor_real,
    valor_real / nullif(sum(valor_real) over (partition by exercicio), 0) as participacao
from por_origem
