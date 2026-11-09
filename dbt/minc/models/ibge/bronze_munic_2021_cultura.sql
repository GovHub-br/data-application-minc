-- Bronze ibge — cópia fiel de seeds.ibge_munic_2021_cultura, como a ibge_perfil_dag grava
-- a aba "Cultura" da MUNIC 2021.
-- Sem cast, filtro nem join (ADR 0010): tudo continua text. Quem tipa é
-- silver_munic_2021_cultura.
select * from {{ source("ibge", "munic_2021_cultura") }}  -- noqa: AM04
