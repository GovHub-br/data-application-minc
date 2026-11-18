-- Bronze ibge — cópia fiel de seeds.ibge_estadic_cultura_2014_politicas_culturais, como
-- a ibge_perfil_dag grava
-- a aba "Políticas culturais" da ESTADIC 2014, suplemento de cultura.
-- Sem cast, filtro nem join (ADR 0010): tudo continua text. Quem tipa é
-- silver_estadic_cultura_2014_politicas_culturais.
select * from {{ source("ibge", "estadic_cultura_2014_politicas_culturais") }}  -- noqa: AM04
