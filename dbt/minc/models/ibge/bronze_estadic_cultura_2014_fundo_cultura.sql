-- Bronze ibge — cópia fiel de seeds.ibge_estadic_cultura_2014_fundo_cultura, como a
-- ibge_perfil_dag grava
-- a aba "Fundo cultura" da ESTADIC 2014, suplemento de cultura.
-- Sem cast, filtro nem join (ADR 0010): tudo continua text. Quem tipa é
-- silver_estadic_cultura_2014_fundo_cultura.
select * from {{ source("ibge", "estadic_cultura_2014_fundo_cultura") }}  -- noqa: AM04
