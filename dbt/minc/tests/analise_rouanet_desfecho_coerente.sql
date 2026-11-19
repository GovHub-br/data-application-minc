-- O desfecho, a fonte e a data tem de contar a mesma historia: 'sem desfecho' se
-- e so se nao ha fonte, e data_desfecho e a primeira data da classe final. Hoje
-- vale por construcao. Uma linha aqui quer dizer que uma dessas colunas mudou de
-- regra sem a outra.
select pronac
from {{ ref("desfecho_prestacao_contas") }}
where
    (desfecho_pc = 'sem desfecho') <> (fonte is null)
    or data_desfecho is distinct from (
        case
            desfecho_pc
            when 'aprovada'
            then data_primeira_aprovada
            when 'aprovada com prejuízo'
            then data_primeira_com_prejuizo
            when 'reprovada ou TCE'
            then data_primeira_reprovada
        end
    )
