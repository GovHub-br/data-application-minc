-- Silver transferegov — perfil dos agentes da LPG e da PNAB num modelo só: o union
-- all das cinco silvers de perfil, sem filtro, sem dedup e sem regra. Um agente que
-- aparece em mais de uma planilha fica com uma linha por aparição; quem escolhe a
-- linha é a gold eixo2_meta3_perfil_agente.
-- cidade/uf só existem nas silvers da LPG; as da PNAB não trazem localização do
-- agente e ficam NULL aqui (ausência, não "fora de território").
-- View: as cinco silvers já são `table` e já cortaram o lixo de parsing.
select
    identificador_unico,
    tipo_proponente,
    origem,
    raca_bruto,
    pcd_bruto,
    indigena_bruto,
    quilombola_bruto,
    cidade,
    uf
from {{ ref("silver_agentes_pf") }}
union all
select
    identificador_unico,
    tipo_proponente,
    origem,
    raca_bruto,
    pcd_bruto,
    indigena_bruto,
    quilombola_bruto,
    cidade,
    uf
from {{ ref("silver_agentes_pj") }}
union all
select
    identificador_unico,
    tipo_proponente,
    origem,
    raca_bruto,
    pcd_bruto,
    indigena_bruto,
    quilombola_bruto,
    cidade,
    uf
from {{ ref("silver_agentes_coletivos") }}
union all
select
    identificador_unico,
    tipo_proponente,
    origem,
    raca_bruto,
    pcd_bruto,
    indigena_bruto,
    quilombola_bruto,
    cast(null as text) as cidade,
    cast(null as text) as uf
from {{ ref("silver_agentes_pnab_pf") }}
union all
select
    identificador_unico,
    tipo_proponente,
    origem,
    raca_bruto,
    pcd_bruto,
    indigena_bruto,
    quilombola_bruto,
    cast(null as text) as cidade,
    cast(null as text) as uf
from {{ ref("silver_agentes_pnab_pj") }}
