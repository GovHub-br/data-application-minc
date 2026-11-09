-- Gold do Eixo 1, frente ESTADIC: a proporção de estados com legislação de
-- patrimônio cultural em 2014, 2018 e 2021 (gráfico 6.3 do ESTADIC.R, salvo como
-- grafico_7_1). Só conta resposta 'Sim' ou 'Não'.
select
    ano,
    count(*) filter (where tem_lei_patrimonio = 'Sim') as quantidade_ufs_sim,
    count(*) as quantidade_ufs,
    avg(case when tem_lei_patrimonio = 'Sim' then 1 else 0 end) as proporcao
from {{ ref("eixo1_estadic_painel_uf") }}
where ano in (2014, 2018, 2021) and tem_lei_patrimonio in ('Sim', 'Não')
group by ano
