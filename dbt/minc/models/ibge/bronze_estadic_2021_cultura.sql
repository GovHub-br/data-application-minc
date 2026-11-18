-- Bronze ibge — cópia fiel de seeds.ibge_estadic_2021_cultura, como a ibge_perfil_dag
-- grava
-- a aba "Cultura" da ESTADIC 2021.
-- Sem cast, filtro nem join (ADR 0010): tudo continua text. Quem tipa é
-- silver_estadic_2021_cultura.
select * from {{ source("ibge", "estadic_2021_cultura") }}  -- noqa: AM04
