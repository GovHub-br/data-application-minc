-- Bronze ibge — cópia fiel de seeds.ibge_estadic_2024_igualdade_racial, como a
-- ibge_perfil_dag grava
-- a aba "Igualdade Racial" da ESTADIC 2024.
-- Sem cast, filtro nem join (ADR 0010): tudo continua text. Quem tipa é
-- silver_estadic_2024_igualdade_racial.
select * from {{ source("ibge", "estadic_2024_igualdade_racial") }}  -- noqa: AM04
