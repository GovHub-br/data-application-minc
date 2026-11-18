-- Bronze ibge — cópia fiel de seeds.ibge_estadic_2018_cultura, como a ibge_perfil_dag
-- grava
-- a aba "Cultura" da ESTADIC 2018.
-- Sem cast, filtro nem join (ADR 0010): tudo continua text. Quem tipa é
-- silver_estadic_2018_cultura.
select * from {{ source("ibge", "estadic_2018_cultura") }}  -- noqa: AM04
