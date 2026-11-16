-- A Figura 15 junta a MUNIC e o SNC. O ponto do SNC vem da linha 'Conselho
-- paritário?' de eixo1_snc_perfil_conselho: se o rótulo mudar lá, ele some
-- daqui sem erro, e é isso que este teste pega.
with
    esperadas as (
        select 'MUNIC' as fonte
        union all
        select 'SNC' as fonte
    )

select esperadas.fonte
from esperadas
where
    esperadas.fonte
    not in (select serie.fonte from {{ ref("eixo1_paridade_conselho_serie") }} as serie)
