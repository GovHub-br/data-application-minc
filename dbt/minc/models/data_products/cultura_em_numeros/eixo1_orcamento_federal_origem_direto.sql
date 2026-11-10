-- Gold do Eixo 1, frente orçamento, esfera federal: o gasto federal direto em
-- cultura por exercício e origem do recurso, valor empenhado nominal (gráfico I
-- do gasto_federal.R, grafico_origem_direto, que usa só o SIOP). Como no R, a
-- linha sem UO fica de fora deste gráfico, e a soma inclui a origem que somou
-- zero no exercício.
--
-- A Figura 1 da publicação parte destes valores, mas leva a LPG de 2022 para
-- 2023 e soma a renúncia fiscal (SALIC e Ancine), que não está aqui.
select ano, categoria_origem, sum(empenhado) as valor_empenhado
from {{ ref("eixo1_orcamento_federal_empenho_cultura") }}
where uo_cod is not null
group by ano, categoria_origem
