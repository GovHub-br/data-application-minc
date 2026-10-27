-- Gold — Meta 3: chaves distintas de eixo2_meta3_perfil_agente (para teste de unicidade
-- e contagem secundária).
select distinct identificador_unico, tipo_proponente, origem
from {{ ref("eixo2_meta3_perfil_agente") }}
