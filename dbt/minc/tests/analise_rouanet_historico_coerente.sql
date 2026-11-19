-- Projeto anterior que conta como aprovado, captou ou tem prestação de contas
-- também tem de contar como existente: toda medida do historico_proponente é um
-- subconjunto de qt_projetos_anteriores. Uma linha aqui quer dizer que alguma
-- medida passou a olhar para depois da data de referência, ou que a regra de
-- "existia" deixou de cobrir um evento.
select pronac
from {{ ref("historico_proponente") }}
where
    qt_aprovados_anteriores > qt_projetos_anteriores
    or qt_captaram_anteriores > qt_projetos_anteriores
    or qt_pc_aprovadas_anteriores + qt_pc_com_problema_anteriores > qt_projetos_anteriores
    or anos_desde_primeiro_projeto < 0
