-- Gold — Figs. 7 e 8 da publicação: o investimento municipal em cultura por
-- exercício e origem do recurso (Fig. 7, em reais correntes) e a participação
-- de cada origem no total do ano (Fig. 8).
--
-- Diferente do estadual, as emendas parlamentares entram nas duas figuras,
-- como em gasto_municipal.R. A participação é sobre o valor real, como no R;
-- dentro de um ano é a mesma do valor nominal.
--
-- Só agrega eixo1_orcamento_municipal_municipio_fonte. Sem a LAB 1 enquanto
-- ela não entrar lá.
-- GRÃO: uma linha por (exercicio, origem) que tem empenho.
with
    por_origem as (
        select
            exercicio,
            origem,
            fonte_recurso,
            sum(valor_nominal) as valor_nominal,
            sum(valor_real) as valor_real,
            count(*) as municipios
        from {{ ref("eixo1_orcamento_municipal_municipio_fonte") }}
        group by exercicio, origem, fonte_recurso
    )

select
    exercicio,
    origem,
    fonte_recurso,
    valor_nominal,
    valor_real,
    municipios,
    valor_real / nullif(sum(valor_real) over (partition by exercicio), 0) as participacao
from por_origem
