-- Gold — empenho estadual em cultura por exercício, UF e origem do recurso, em
-- reais correntes e de 2024. Todas as origens, inclusive as emendas
-- parlamentares, que as Figs. 4 e 5 tiram. É o estadual_final.csv de
-- gasto_estadual.R.
--
-- Só agrega eixo1_orcamento_empenho_cultura, onde está a regra.
-- GRÃO: uma linha por (exercicio, uf, origem) que tem empenho.
select
    exercicio,
    uf,
    cod_ibge,
    origem,
    case
        origem
        when 'proprio'
        then 'Execução direta Estadual (recursos próprios)'
        when 'emenda'
        then 'Emendas Parlamentares'
        when 'lab'
        then 'Lei Aldir Blanc (LAB)'
        when 'lpg'
        then 'Lei Paulo Gustavo (LPG)'
        when 'pnab'
        then 'Política Nacional Aldir Blanc (PNAB)'
    end as fonte_recurso,
    sum(valor_nominal) as valor_nominal,
    sum(valor_real) as valor_real,
    count(*) as linhas_msc
from {{ ref("eixo1_orcamento_empenho_cultura") }}
where esfera_eixo1 = 'estadual'
group by exercicio, uf, cod_ibge, origem
