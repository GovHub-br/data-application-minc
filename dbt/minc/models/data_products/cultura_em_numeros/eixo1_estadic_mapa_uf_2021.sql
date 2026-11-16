-- Gold do Eixo 1, frente ESTADIC: uma linha por UF em 2021 com as categorias
-- dos mapas 8.1 e 8.2 do ESTADIC.R. Desenhar o mapa fica com o consumidor. O
-- perfil_fomento do mapa 8.3 foi retirado até a #176, porque a lei de
-- incentivo que ele cruza vem da ecul31, que não é essa pergunta (ver o
-- schema.yml).
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
    maturidade_snc
from {{ ref("eixo1_estadic_painel_uf") }}
where ano = 2021
