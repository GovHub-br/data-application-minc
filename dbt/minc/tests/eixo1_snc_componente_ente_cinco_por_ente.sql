-- A gold-base de componentes do SNC tem os cinco componentes de cada ente.
-- Ente com menos é componente que some da Tabela 1.2 e das Figuras 12 a 14
-- sem erro; com mais, ente contado duas vezes.
select cod_ibge, count(*) as componentes
from {{ ref("eixo1_snc_componente_ente") }}
group by cod_ibge
having count(*) <> 5
