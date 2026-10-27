-- Bronze transferegov — cópia fiel de transferegov.territorio_fcu_setores, o
-- crosswalk IBGE (Censo 2022) de setor censitário para Favela e Comunidade
-- Urbana. Sem cast, filtro nem join (ADR 0010). Quem colapsa para município é
-- silver_territorio_municipio.
select * from {{ source("transferegov", "territorio_fcu_setores") }}  -- noqa: AM04
