-- Bronze ibge — cópia fiel de seeds.ibge_estadic_2023_primeira_infancia, como a
-- ibge_perfil_dag grava
-- a aba "Primeira Infância" da ESTADIC 2023.
-- Sem cast, filtro nem join (ADR 0010): tudo continua text. Quem tipa é
-- silver_estadic_2023_primeira_infancia.
select * from {{ source("ibge", "estadic_2023_primeira_infancia") }}  -- noqa: AM04
