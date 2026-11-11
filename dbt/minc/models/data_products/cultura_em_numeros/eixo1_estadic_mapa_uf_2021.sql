-- Gold do Eixo 1, frente ESTADIC: uma linha por UF em 2021 com as três
-- categorias dos mapas do ESTADIC.R (mapas 8.1 a 8.3). Desenhar o mapa fica com
-- o consumidor.
--
-- O tripé (instrumentos_do_tripe e maturidade_snc) vem pronto do
-- eixo1_estadic_painel_uf, que o calcula uma vez. Diferença do R: lá a soma
-- nunca é NULL, porque a fix_sim_nao faz do vazio 'Não'; aqui, com um item
-- vazio, a contagem fica NULL e a maturidade também, salvo quando os outros
-- dois são 'Sim' e 'Não' (parcial de qualquer jeito). Em 2021 nenhuma UF tem
-- item do tripé vazio.
select
    cod_uf,
    sigla_uf,
    nome_uf,
    regiao,
    tipo_orgao_gestor,
    instrumentos_do_tripe,
    maturidade_snc,
    case
        when tem_lei_patrimonio = 'Sim' and tem_lei_incentivo = 'Sim'
        then 'Possui Ambas as Leis'
        when tem_lei_patrimonio = 'Sim' and tem_lei_incentivo = 'Não'
        then 'Apenas Lei de Patrimônio'
        when tem_lei_patrimonio = 'Não' and tem_lei_incentivo = 'Sim'
        then 'Apenas Lei de Incentivo'
        else 'Não Possui/Sem Informação'
    end as perfil_fomento
from {{ ref("eixo1_estadic_painel_uf") }}
where ano = 2021
